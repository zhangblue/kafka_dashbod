pub mod health;

use axum::{Router, routing::get};

pub fn router() -> Router {
    Router::new().route("/api/health", get(health::health))
}
