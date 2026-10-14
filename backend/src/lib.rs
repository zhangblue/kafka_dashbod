pub mod api;
pub mod config;
pub mod crypto;
pub mod db;
pub mod domain;

use axum::Router;

pub fn app_for_test() -> Router {
    api::router()
}
