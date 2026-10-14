pub mod api;

use axum::Router;

pub fn app_for_test() -> Router {
    api::router()
}
