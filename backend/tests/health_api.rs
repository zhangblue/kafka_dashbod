use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use tower::ServiceExt;

// 验证健康接口返回约定的成功 JSON，供启动就绪检测依赖。
#[tokio::test]
async fn health_returns_ok_json() {
    let response = kafka_dashboard::app_for_test()
        .oneshot(
            Request::builder()
                .uri("/api/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], br#"{"status":"ok"}"#);
}
