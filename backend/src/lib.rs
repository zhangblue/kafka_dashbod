pub mod api;
pub mod config;
pub mod db;
pub mod domain;
pub mod error;
pub mod kafka;
pub mod notifications;
pub mod sessions;

use axum::Router;

pub fn app_for_test() -> Router {
    api::router()
}
