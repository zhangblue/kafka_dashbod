#![allow(dead_code)]

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, header::CONTENT_TYPE},
    response::Response,
};
use kafka_dashboard::db::{self, clusters::ClusterRepository};
use kafka_dashboard::notifications::feishu::AlertSender;
use serde_json::{Value, json};
use tower::ServiceExt;

pub struct TestApp {
    pub app: Router,
    pub pool: sqlx::SqlitePool,
    pub clusters: ClusterRepository,
}

// 为普通 API 测试创建独立内存数据库与路由，避免持久化或跨测试污染。
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

// 在告警 API 准备阶段注入受控发送器，隔离真实通知网络副作用。
pub async fn test_app_with_sender(sender: impl AlertSender + 'static) -> TestApp {
    let pool = db::connect("sqlite::memory:").await.unwrap();
    let clusters = ClusterRepository::new(pool.clone());
    let app = kafka_dashboard::api::router_with_state(
        kafka_dashboard::api::AppState::new(pool.clone())
            .with_alert_sender(std::sync::Arc::new(sender)),
    );
    TestApp {
        app,
        pool,
        clusters,
    }
}

// 在 API 测试执行阶段构造可选 JSON 请求并调用内存路由，统一请求入口。
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

// 在响应断言阶段读取完整正文，供状态、安全错误及敏感值检查使用。
pub async fn response_text(response: Response) -> String {
    String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}

// 在 API 断言阶段解析响应正文，复用统一的正文读取流程。
pub async fn response_json(response: Response) -> Value {
    serde_json::from_str(&response_text(response).await).unwrap()
}

// 在 API 测试准备阶段构造不含凭据的明文连接输入。
pub fn cluster(name: &str) -> Value {
    json!({"name": name, "bootstrapServers": "broker:9092", "securityProtocol": "PLAINTEXT"})
}

// 在依赖集群的 API 测试准备阶段创建配置并返回其真实标识。
pub async fn create_cluster(app: &Router, name: &str) -> String {
    let response = request(app, "POST", "/api/clusters", Some(cluster(name))).await;
    assert_eq!(response.status(), 201);
    response_json(response).await["id"]
        .as_str()
        .unwrap()
        .to_owned()
}
