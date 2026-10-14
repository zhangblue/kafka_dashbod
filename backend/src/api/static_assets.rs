use std::path::{Component, Path, PathBuf};

use axum::{
    extract::Request,
    http::{HeaderValue, Method, StatusCode, header::CACHE_CONTROL},
    response::{IntoResponse, Response},
};
use percent_encoding::percent_decode_str;
use tower_http::services::{ServeDir, ServeFile};

use crate::error::{self, ApiError};

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

async fn safe_file(root: &Path, path: &Path) -> bool {
    tokio::fs::canonicalize(path).await.is_ok_and(|target| {
        target
            .strip_prefix(root)
            .is_ok_and(|relative| !has_private_component(relative))
            && target.is_file()
    })
}

pub async fn serve(root: PathBuf, request: Request) -> Response {
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
    let Ok(root) = tokio::fs::canonicalize(root).await else {
        return error::not_found().await.into_response();
    };
    let candidate = root.join(path.trim_start_matches('/'));
    // ServeDir rejects parent traversal, but follows symlinks and permits dotfiles.
    // Check canonical targets as well as URL components before giving it a request.
    match tokio::fs::canonicalize(&candidate).await {
        Ok(target) => {
            if !target
                .strip_prefix(&root)
                .is_ok_and(|relative| !has_private_component(relative))
            {
                return error::not_found().await.into_response();
            }
        }
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound
                || error.kind() == std::io::ErrorKind::NotADirectory => {}
        Err(_) => return error::not_found().await.into_response(),
    }
    let index = root.join("index.html");
    if !safe_file(&root, &index).await {
        return error::not_found().await.into_response();
    }
    let mut files = ServeDir::new(&root)
        .append_index_html_on_directories(false)
        .fallback(ServeFile::new(index));
    match files.try_call(request).await {
        Ok(response) => {
            let mut response = response.into_response();
            // Revalidate both shell and assets so a deployment cannot pin stale HTML
            // or non-fingerprinted assets in a browser cache.
            response
                .headers_mut()
                .insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
            response
        }
        Err(_) => ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
            "The requested asset could not be served",
        )
        .into_response(),
    }
}
