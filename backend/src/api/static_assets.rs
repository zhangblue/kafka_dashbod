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

fn is_api(path: &str) -> bool {
    path == "/api" || path.starts_with("/api/")
}

fn has_private_component(path: &Path) -> bool {
    path.components().any(|component| match component {
        Component::Normal(value) => value.to_string_lossy().starts_with('.'),
        Component::ParentDir => true,
        _ => false,
    })
}

pub(crate) async fn serve(root: StaticRoot, #[allow(unused_mut)] mut request: Request) -> Response {
    let Ok(path) = percent_decode_str(request.uri().path()).decode_utf8() else {
        return error::not_found().await.into_response();
    };
    // API misses must never read a file or receive the SPA shell, for any method.
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
            // Revalidate both shell and assets so a deployment cannot pin stale HTML
            // or non-fingerprinted assets in a browser cache.
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

    // Replaces a validated regular file at the exact handoff to the file opener.
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

    // The configured path may change, but serving must stay anchored to the opened root.
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
