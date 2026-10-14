use std::{path::Path, process::Command};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
    response::Response,
};
use kafka_dashboard::{api::AppState, crypto::SecretCipher};
use tower::ServiceExt;

const INDEX: &str = "<main>dashboard</main>";
const KEY: &str = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=";

fn web_root() -> tempfile::TempDir {
    let web = tempfile::tempdir().unwrap();
    std::fs::create_dir(web.path().join("assets")).unwrap();
    std::fs::write(web.path().join("index.html"), INDEX).unwrap();
    std::fs::write(web.path().join("assets/app.js"), "console.log('ok')").unwrap();
    std::fs::write(web.path().join(".env"), "fixture-secret").unwrap();
    web
}

async fn app(web: &Path) -> Router {
    let pool = kafka_dashboard::db::connect("sqlite::memory:")
        .await
        .unwrap();
    let state = AppState::new(pool, SecretCipher::from_base64_key(KEY).unwrap());
    kafka_dashboard::api::router_with_state_and_web_root(state, Some(web.to_owned()))
}

async fn request(app: &Router, method: &str, path: &str) -> Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn text(response: Response) -> String {
    String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}

// Fails if static requests keep reaching the API 404 or HTML is sent as JavaScript.
#[tokio::test]
async fn serves_real_assets_with_mime_and_revalidates_cached_files() {
    let web = web_root();
    let app = app(web.path()).await;
    let response = request(&app, "GET", "/assets/app.js?v=1").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers()["content-type"]
            .to_str()
            .unwrap()
            .contains("javascript")
    );
    assert_eq!(response.headers()["cache-control"], "no-cache");
    assert_eq!(text(response).await, "console.log('ok')");
    let response = request(&app, "HEAD", "/assets/app.js").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(text(response).await.is_empty());
}

// Fails if deep links and missing static files cannot load the SPA shell.
#[tokio::test]
async fn non_api_get_paths_fall_back_to_uncached_html() {
    let web = web_root();
    let app = app(web.path()).await;
    for path in [
        "/",
        "/index.html",
        "/clusters/demo/topics",
        "/assets/missing.js",
    ] {
        let response = request(&app, "GET", path).await;
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert!(
            response.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with("text/html")
        );
        assert_eq!(response.headers()["cache-control"], "no-cache");
        assert_eq!(text(response).await, INDEX);
    }
    assert_eq!(
        request(&app, "POST", "/clusters/demo/topics")
            .await
            .status(),
        StatusCode::METHOD_NOT_ALLOWED
    );
}

// Fails if an API miss is swallowed by the shell or shadows a real API route.
#[tokio::test]
async fn api_routes_take_precedence_and_unknown_api_paths_always_return_json_404() {
    let web = web_root();
    std::fs::create_dir(web.path().join("api")).unwrap();
    std::fs::write(web.path().join("api/not-found"), "must not serve").unwrap();
    let app = app(web.path()).await;
    for path in [
        "/api",
        "/api/",
        "/api/not-found",
        "/api/clusters/missing/unregistered",
    ] {
        for method in ["GET", "POST", "HEAD", "OPTIONS"] {
            let response = request(&app, method, path).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method} {path}");
            assert_eq!(response.headers()["content-type"], "application/json");
            assert!(response.headers().contains_key("x-request-id"));
            if method != "HEAD" {
                let error: serde_json::Value = serde_json::from_str(&text(response).await).unwrap();
                assert_eq!(error["code"], "NOT_FOUND");
            }
        }
    }
    assert_eq!(
        text(request(&app, "GET", "/api/health").await).await,
        r#"{"status":"ok"}"#
    );
    assert_eq!(
        text(request(&app, "GET", "/api/clusters").await).await,
        "[]"
    );
}

// Fails if percent encoding, hidden files, or parent traversal exposes private files.
#[tokio::test]
async fn rejects_hidden_paths_and_encoded_traversal() {
    let web = web_root();
    let app = app(web.path()).await;
    for path in [
        "/.env",
        "/%2eenv",
        "/assets/../.env",
        "/assets/%2e%2e/.env",
        "/%2e%2e/outside",
        "/assets%5c..%5c.env",
    ] {
        let response = request(&app, "GET", path).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        assert!(!text(response).await.contains("fixture-secret"));
    }
}

#[cfg(unix)]
#[tokio::test]
async fn rejects_symlinks_outside_root_and_to_hidden_files() {
    let web = web_root();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("secret"), "fixture-outside-secret").unwrap();
    std::os::unix::fs::symlink(outside.path(), web.path().join("escape")).unwrap();
    std::os::unix::fs::symlink(web.path().join(".env"), web.path().join("public.txt")).unwrap();
    let app = app(web.path()).await;
    for path in ["/escape/secret", "/public.txt"] {
        assert_eq!(
            request(&app, "GET", path).await.status(),
            StatusCode::NOT_FOUND
        );
    }
}

// Fails if version processing requires a database, a valid key, or a web directory.
#[test]
fn version_exits_without_initializing_configuration() {
    let output = Command::new(env!("CARGO_BIN_EXE_kafka-dashboard"))
        .arg("--version")
        .env_remove("KAFKA_DASHBOARD_MASTER_KEY")
        .env("DATABASE_URL", "invalid://not-a-database")
        .env("KAFKA_DASHBOARD_WEB_DIR", "/does/not/exist")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains(env!("CARGO_PKG_VERSION"))
    );
}

// Fails if an explicit broken deployment silently starts without serving its UI.
#[test]
fn startup_rejects_missing_web_directory_or_index_before_database_initialization() {
    let parent = tempfile::tempdir().unwrap();
    let not_a_file = parent.path().join("index.html");
    std::fs::create_dir(&not_a_file).unwrap();
    for path in [
        parent.path().join("missing"),
        parent.path().to_owned(),
        not_a_file,
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_kafka-dashboard"))
            .env("KAFKA_DASHBOARD_MASTER_KEY", KEY)
            .env("DATABASE_URL", "invalid://not-a-database")
            .env("KAFKA_DASHBOARD_WEB_DIR", &path)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("KAFKA_DASHBOARD_WEB_DIR"),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
