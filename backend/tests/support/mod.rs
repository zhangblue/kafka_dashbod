#![allow(dead_code)]

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, header::CONTENT_TYPE},
    response::Response,
};
use kafka_dashboard::db::{self, clusters::ClusterRepository};
use serde_json::{Value, json};
use tower::ServiceExt;

pub struct TestApp {
    pub app: Router,
    pub pool: sqlx::SqlitePool,
    pub clusters: ClusterRepository,
}

pub async fn test_app() -> TestApp {
    let pool = db::connect("sqlite::memory:").await.unwrap();
    let clusters = ClusterRepository::new(pool.clone());
    let app =
        kafka_dashboard::api::router_with_state(kafka_dashboard::api::AppState::new(pool.clone()));
    TestApp {
        app,
        pool,
        clusters,
    }
}

pub async fn request(app: &Router, method: &str, uri: &str, value: Option<Value>) -> Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(CONTENT_TYPE, "application/json")
                .body(
                    value
                        .map(|value| Body::from(value.to_string()))
                        .unwrap_or_else(Body::empty),
                )
                .unwrap(),
        )
        .await
        .unwrap()
}

pub async fn response_text(response: Response) -> String {
    String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}

pub async fn response_json(response: Response) -> Value {
    serde_json::from_str(&response_text(response).await).unwrap()
}

pub fn cluster(name: &str) -> Value {
    json!({"name": name, "bootstrapServers": "broker:9092", "securityProtocol": "PLAINTEXT"})
}

pub async fn create_cluster(app: &Router, name: &str) -> String {
    let response = request(app, "POST", "/api/clusters", Some(cluster(name))).await;
    assert_eq!(response.status(), 201);
    response_json(response).await["id"]
        .as_str()
        .unwrap()
        .to_owned()
}
