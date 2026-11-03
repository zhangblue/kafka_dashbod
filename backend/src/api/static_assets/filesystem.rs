use std::{
    fs::{File as StdFile, Metadata},
    future::Future,
    io,
    path::{Component, Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use rustix::fs::{Mode, OFlags, open, openat};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};
use tower_http::services::fs::{Backend, File};

type IoFuture<T> = Pin<Box<dyn Future<Output = io::Result<T>> + Send>>;

/// 应用启动时固定的目录能力，请求期间不重新解析根路径。
#[derive(Clone)]
pub(crate) struct StaticRoot(Arc<StdFile>);

// 在路径不满足静态资源安全边界时生成权限错误，阻止继续尝试 SPA 回退。
fn denied() -> io::Error {
    io::Error::from(io::ErrorKind::PermissionDenied)
}

// 在逐级打开资源失败时归一化符号链接错误，避免把安全拒绝误判为文件缺失。
fn open_error(error: rustix::io::Errno) -> io::Error {
    // 中间组件是符号链接时 NOFOLLOW 也可能返回 NOTDIR；两者均按拒绝处理，禁止回退首页。
    if error == rustix::io::Errno::LOOP || error == rustix::io::Errno::NOTDIR {
        denied()
    } else {
        error.into()
    }
}

impl StaticRoot {
    // 在启动时固定静态根目录的打开句柄，并验证首页为普通文件以便尽早发现部署错误。
    pub(crate) fn new(path: &Path) -> io::Result<Self> {
        // 只规范化一次根目录，再以 NOFOLLOW 逐级打开，阻止组件被替换成符号链接的竞态。
        let path = path.canonicalize()?;
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let mut directory = StdFile::from(open("/", flags, Mode::empty())?);
        for component in path.components() {
            if let Component::Normal(name) = component {
                directory = StdFile::from(openat(&directory, name, flags, Mode::empty())?);
            }
        }
        let root = Self(Arc::new(directory));
        let index = root.open_relative(Path::new("index.html"))?;
        if !index.metadata()?.is_file() {
            return Err(denied());
        }
        Ok(root)
    }

    // 在每次资源打开或元数据查询时相对固定目录逐级打开，拒绝链接、私有组件和特殊文件。
    fn open_relative(&self, path: &Path) -> io::Result<StdFile> {
        let mut names = Vec::new();
        for component in path.components() {
            match component {
                Component::CurDir => {}
                Component::Normal(name) if !name.to_string_lossy().starts_with('.') => {
                    names.push(name)
                }
                _ => return Err(denied()),
            }
        }
        let mut directory = self.0.try_clone()?;
        for (index, name) in names.iter().enumerate() {
            let mut flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK;
            if index + 1 < names.len() {
                flags |= OFlags::DIRECTORY;
            }
            directory =
                StdFile::from(openat(&directory, *name, flags, Mode::empty()).map_err(open_error)?);
        }
        // 属性校验针对刚打开的精确描述符，不重新打开路径，防止校验后被替换。
        let metadata = directory.metadata()?;
        if !metadata.is_file() && !metadata.is_dir() {
            return Err(denied());
        }
        Ok(directory)
    }
}

#[derive(Clone)]
pub(super) struct SecureBackend {
    root: StaticRoot,
    #[cfg(test)]
    pub(super) before_open: Option<super::BeforeOpen>,
}

impl SecureBackend {
    // 在静态请求开始时构造共享根目录的安全文件后端，供资源服务执行读取。
    pub(super) fn new(root: StaticRoot) -> Self {
        Self {
            root,
            #[cfg(test)]
            before_open: None,
        }
    }
}

impl Backend for SecureBackend {
    type File = SecureFile;
    type Metadata = Metadata;
    type OpenFuture = IoFuture<SecureFile>;
    type MetadataFuture = IoFuture<Metadata>;

    // 在资源服务请求文件时转移阻塞打开工作，再将已验证句柄交给异步读取。
    fn open(&self, path: PathBuf) -> Self::OpenFuture {
        let root = self.root.clone();
        #[cfg(test)]
        if let Some(hook) = &self.before_open {
            (hook.0)();
        }
        Box::pin(async move {
            // 文件系统打开可能阻塞，移交阻塞线程并等待结果后才交付异步文件。
            let file = tokio::task::spawn_blocking(move || root.open_relative(&path)).await??;
            Ok(SecureFile(tokio::fs::File::from_std(file)))
        })
    }

    // 在资源服务检查路径元数据时沿安全打开流程取得属性，不绕过目录能力边界。
    fn metadata(&self, path: PathBuf) -> Self::MetadataFuture {
        let root = self.root.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || root.open_relative(&path)?.metadata()).await?
        })
    }
}

pub(super) struct SecureFile(tokio::fs::File);

impl AsyncRead for SecureFile {
    // 在响应体读取时转发异步读取轮询，始终使用安全打开得到的同一文件句柄。
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buffer)
    }
}

impl AsyncSeek for SecureFile {
    // 在范围响应需要定位时转发异步文件的起始寻址操作。
    fn start_seek(mut self: Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
        Pin::new(&mut self.0).start_seek(position)
    }

    // 在范围响应等待寻址完成时转发轮询结果。
    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Pin::new(&mut self.0).poll_complete(cx)
    }
}

impl File for SecureFile {
    type Metadata = Metadata;
    type MetadataFuture<'a> = Pin<Box<dyn Future<Output = io::Result<Metadata>> + Send + 'a>>;

    // 在资源服务读取文件属性时查询已打开句柄，避免重新解析请求路径。
    fn metadata(&self) -> Self::MetadataFuture<'_> {
        Box::pin(self.0.metadata())
    }
}
