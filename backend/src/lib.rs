pub mod alerts;
pub mod api;
pub mod config;
pub mod db;
pub mod domain;
pub mod error;
pub mod kafka;
pub mod notifications;
pub mod sessions;

use axum::Router;

// 在基础 HTTP 测试中提供应用入口，复用生产 API 路由定义。
pub fn app_for_test() -> Router {
    api::router()
}
