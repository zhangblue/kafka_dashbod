pub mod api;
pub mod domain;

use axum::Router;

pub fn app_for_test() -> Router {
    api::router()
}
