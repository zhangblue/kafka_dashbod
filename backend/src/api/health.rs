use axum::Json;
use serde_json::{Value, json};

// 处理健康检查请求，供启动验证及存活探测确认 HTTP 服务可响应。
pub async fn health() -> Json<Value> {
    Json(json!({"status": "ok"}))
}
