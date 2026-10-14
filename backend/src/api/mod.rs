pub mod clusters;
pub mod health;
pub mod lag_rules;
pub mod sessions;
pub mod static_assets;

use std::path::PathBuf;

use crate::{
    crypto::SecretCipher,
    db::{clusters::ClusterRepository, lag_rules::LagRuleRepository},
    error,
    sessions::manager::{
        KafkaSessionClientFactory, LEASE_DURATION, SessionClientFactory, SessionManager,
    },
};
use axum::{
    Extension, Router,
    extract::DefaultBodyLimit,
    middleware,
    routing::{delete, get, post},
};

#[derive(Clone)]
pub struct AppState<F: SessionClientFactory = KafkaSessionClientFactory> {
    pub clusters: ClusterRepository,
    pub lag_rules: LagRuleRepository,
    pub sessions: SessionManager<F>,
}

impl AppState<KafkaSessionClientFactory> {
    pub fn new(pool: sqlx::SqlitePool, cipher: SecretCipher) -> Self {
        Self {
            clusters: ClusterRepository::new(pool.clone(), cipher),
            lag_rules: LagRuleRepository::new(pool),
            sessions: SessionManager::new(LEASE_DURATION, KafkaSessionClientFactory),
        }
    }
}

impl<F: SessionClientFactory> AppState<F> {
    pub fn with_session_manager<G: SessionClientFactory>(
        self,
        sessions: SessionManager<G>,
    ) -> AppState<G> {
        AppState {
            clusters: self.clusters,
            lag_rules: self.lag_rules,
            sessions,
        }
    }
}

pub fn router_with_state<F: SessionClientFactory>(state: AppState<F>) -> Router {
    router_with_state_and_web_root(state, None)
}

pub fn router_with_state_and_web_root<F: SessionClientFactory>(
    state: AppState<F>,
    web_root: Option<PathBuf>,
) -> Router {
    routes::<F>(web_root)
        .layer(Extension(state.clusters))
        .layer(Extension(state.lag_rules))
        .layer(Extension(state.sessions))
}

pub fn router() -> Router {
    routes::<KafkaSessionClientFactory>(None)
}

fn routes<F: SessionClientFactory>(web_root: Option<PathBuf>) -> Router {
    let router = Router::new()
        .route("/api/health", get(health::health))
        .route("/api/clusters", get(clusters::list).post(clusters::create))
        .route("/api/clusters/{id}/test", post(clusters::test_connection))
        .route(
            "/api/clusters/{id}",
            get(clusters::get)
                .put(clusters::update)
                .delete(clusters::delete::<F>),
        )
        .route(
            "/api/clusters/{id}/lag-rules",
            get(lag_rules::get).put(lag_rules::put),
        )
        .route("/api/clusters/{id}/sessions", post(sessions::create::<F>))
        .route("/api/sessions/{id}", delete(sessions::delete::<F>))
        .route("/api/sessions/{id}/snapshot", get(sessions::snapshot::<F>));
    let router = match web_root {
        Some(root) => router.fallback(move |request| static_assets::serve(root.clone(), request)),
        None => router.fallback(error::not_found),
    };
    router
        .method_not_allowed_fallback(error::method_not_allowed)
        .layer(DefaultBodyLimit::max(256 * 1024))
        .layer(middleware::from_fn(error::request_id))
}
