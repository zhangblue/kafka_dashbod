mod filesystem;

use std::{
    io,
    path::{Component, Path},
};

use filesystem::SecureBackend;
pub(crate) use filesystem::StaticRoot;

use axum::{
    extract::Request,
    http::{HeaderValue, Method, StatusCode, header::CACHE_CONTROL},
    response::{IntoResponse, Response},
};
use percent_encoding::percent_decode_str;
use tower_http::services::ServeDir;

use crate::error::{self, ApiError};

#[cfg(test)]
#[derive(Clone)]
struct BeforeOpen(std::sync::Arc<dyn Fn() + Send + Sync>);

// 在静态回退入口识别 API 路径，确保未知 API 仍使用 JSON 错误响应。
fn is_api(path: &str) -> bool {
    path == "/api" || path.starts_with("/api/")
}

// 在读取资源前识别隐藏组件和父目录跳转，拒绝访问私有文件及目录穿越。
fn has_private_component(path: &Path) -> bool {
    path.components().any(|component| match component {
        Component::Normal(value) => value.to_string_lossy().starts_with('.'),
        Component::ParentDir => true,
        _ => false,
    })
}

// 处理已匹配路由之外的请求，先排除 API 和非法路径，再提供资源或允许的 SPA 首页。
pub(crate) async fn serve(root: StaticRoot, #[allow(unused_mut)] mut request: Request) -> Response {
    let Ok(path) = percent_decode_str(request.uri().path()).decode_utf8() else {
        return error::not_found().await.into_response();
    };
    // API 404 优先于方法和文件检查，任何方法的未知 API 都不能读取文件或返回 SPA 首页。
    if is_api(&path) {
        return error::not_found().await.into_response();
    }
    if request.method() != Method::GET && request.method() != Method::HEAD {
        return error::method_not_allowed().await.into_response();
    }
    if path.contains(['\\', '\0']) || has_private_component(Path::new(path.as_ref())) {
        return error::not_found().await.into_response();
    }
    #[allow(unused_mut)]
    let mut backend = SecureBackend::new(root);
    #[cfg(test)]
    {
        backend.before_open = request.extensions_mut().remove::<BeforeOpen>();
    }
    let mut fallback = Request::builder()
        .method(request.method().clone())
        .uri("/index.html")
        .body(axum::body::Body::empty())
        .expect("static fallback request is valid");
    *fallback.headers_mut() = request.headers().clone();
    let mut files = ServeDir::with_backend("", backend).append_index_html_on_directories(false);
    // 只有资源确实不存在才尝试首页，权限拒绝和符号链接错误不得通过 SPA 回退掩盖。
    let result = match files.try_call(request).await {
        Ok(response) if response.status() == StatusCode::NOT_FOUND => {
            files.try_call(fallback).await
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => files.try_call(fallback).await,
        result => result,
    };
    match result {
        Ok(response) => {
            let mut response = response.into_response();
            // 首页和资源均要求重新验证，避免部署后浏览器仍固定使用旧 HTML 或未带指纹的资源。
            response
                .headers_mut()
                .insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
            response
        }
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::PermissionDenied | io::ErrorKind::NotFound
            ) =>
        {
            error::not_found().await.into_response()
        }
        Err(_) => ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
            "The requested asset could not be served",
        )
        .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };

    // 在校验到真正打开文件的交接处替换目标，稳定复现符号链接竞态。
    // 验证校验后文件被替换成符号链接时无法读取根目录外内容，覆盖检查与打开间的竞态。
    #[tokio::test]
    async fn swapping_validated_file_for_symlink_cannot_read_outside_root() {
        let web = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(web.path().join("index.html"), "shell").unwrap();
        let file = web.path().join("public.txt");
        std::fs::write(&file, "public").unwrap();
        let secret = outside.path().join("secret.txt");
        std::fs::write(&secret, "outside-private-data").unwrap();
        let mut request = Request::builder()
            .uri("/public.txt")
            .body(Body::empty())
            .unwrap();
        request
            .extensions_mut()
            .insert(BeforeOpen(std::sync::Arc::new(move || {
                std::fs::remove_file(&file).unwrap();
                std::os::unix::fs::symlink(&secret, &file).unwrap();
            })));
        let response = serve(StaticRoot::new(web.path()).unwrap(), request).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(!String::from_utf8_lossy(&body).contains("outside-private-data"));
    }

    // 配置路径可以被替换，但服务必须始终锚定启动时打开的根目录。
    // 验证启动后根路径被替换时仍使用原目录能力，防止请求被重定向到外部目录。
    #[tokio::test]
    async fn replacing_root_path_does_not_redirect_the_opened_directory_capability() {
        let parent = tempfile::tempdir().unwrap();
        let web = parent.path().join("web");
        std::fs::create_dir(&web).unwrap();
        std::fs::write(web.join("index.html"), "shell").unwrap();
        std::fs::write(web.join("public.txt"), "public").unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("public.txt"), "outside-private-data").unwrap();
        let root = StaticRoot::new(&web).unwrap();
        std::fs::rename(&web, parent.path().join("original")).unwrap();
        std::os::unix::fs::symlink(outside.path(), &web).unwrap();
        let request = Request::builder()
            .uri("/public.txt")
            .body(Body::empty())
            .unwrap();
        let response = serve(root, request).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            to_bytes(response.into_body(), usize::MAX).await.unwrap(),
            "public"
        );
    }
}
