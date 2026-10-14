pub mod clusters;
pub mod health;
pub mod lag_rules;

use crate::{
    crypto::SecretCipher,
    db::{clusters::ClusterRepository, lag_rules::LagRuleRepository},
    error,
};
use axum::{
    Extension, Router, middleware,
    routing::{get, post},
};

#[derive(Clone)]
pub struct AppState {
    pub clusters: ClusterRepository,
    pub lag_rules: LagRuleRepository,
}

impl AppState {
    pub fn new(pool: sqlx::SqlitePool, cipher: SecretCipher) -> Self {
        Self {
            clusters: ClusterRepository::new(pool.clone(), cipher),
            lag_rules: LagRuleRepository::new(pool),
        }
    }
}

pub fn router_with_state(state: AppState) -> Router {
    router()
        .layer(Extension(state.clusters))
        .layer(Extension(state.lag_rules))
}

pub fn router() -> Router {
    Router::new()
        .route("/api/health", get(health::health))
        .route("/api/clusters", get(clusters::list).post(clusters::create))
        .route("/api/clusters/{id}/test", post(clusters::test_connection))
        .route(
            "/api/clusters/{id}",
            get(clusters::get)
                .put(clusters::update)
                .delete(clusters::delete),
        )
        .route(
            "/api/clusters/{id}/lag-rules",
            get(lag_rules::get).put(lag_rules::put),
        )
        .fallback(error::not_found)
        .method_not_allowed_fallback(error::method_not_allowed)
        .layer(middleware::from_fn(error::request_id))
}
