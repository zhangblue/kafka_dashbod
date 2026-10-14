pub mod api;
pub mod config;
pub mod crypto;
pub mod db;
pub mod domain;
pub mod error;

use axum::Router;

pub fn app_for_test() -> Router {
    api::router()
}
