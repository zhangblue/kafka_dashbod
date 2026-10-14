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

/// A directory capability fixed at application startup, never re-resolved by requests.
#[derive(Clone)]
pub(crate) struct StaticRoot(Arc<StdFile>);

fn denied() -> io::Error {
    io::Error::from(io::ErrorKind::PermissionDenied)
}

fn open_error(error: rustix::io::Errno) -> io::Error {
    // NOFOLLOW on an intermediate symlink can return NOTDIR rather than LOOP.
    // Neither condition should turn a rejected path into a successful SPA fallback.
    if error == rustix::io::Errno::LOOP || error == rustix::io::Errno::NOTDIR {
        denied()
    } else {
        error.into()
    }
}

impl StaticRoot {
    pub(crate) fn new(path: &Path) -> io::Result<Self> {
        // Normalize the configured root once. Every component is then opened with
        // NOFOLLOW, so swapping any normalized component for a symlink cannot win.
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
        // fstat applies to the exact descriptor returned above. No path is reopened.
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

    fn open(&self, path: PathBuf) -> Self::OpenFuture {
        let root = self.root.clone();
        #[cfg(test)]
        if let Some(hook) = &self.before_open {
            (hook.0)();
        }
        Box::pin(async move {
            let file = tokio::task::spawn_blocking(move || root.open_relative(&path)).await??;
            Ok(SecureFile(tokio::fs::File::from_std(file)))
        })
    }

    fn metadata(&self, path: PathBuf) -> Self::MetadataFuture {
        let root = self.root.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || root.open_relative(&path)?.metadata()).await?
        })
    }
}

pub(super) struct SecureFile(tokio::fs::File);

impl AsyncRead for SecureFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buffer)
    }
}

impl AsyncSeek for SecureFile {
    fn start_seek(mut self: Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
        Pin::new(&mut self.0).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Pin::new(&mut self.0).poll_complete(cx)
    }
}

impl File for SecureFile {
    type Metadata = Metadata;
    type MetadataFuture<'a> = Pin<Box<dyn Future<Output = io::Result<Metadata>> + Send + 'a>>;

    fn metadata(&self) -> Self::MetadataFuture<'_> {
        Box::pin(self.0.metadata())
    }
}
