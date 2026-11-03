pub mod clusters;
pub mod feishu_alerts;
pub mod health;
pub mod lag_rules;
pub mod sessions;
pub mod static_assets;
pub mod topics;

use std::{path::PathBuf, sync::Arc};

use crate::{
    db::{
        clusters::ClusterRepository, feishu_alerts::FeishuAlertRepository,
        lag_rules::LagRuleRepository,
    },
    error,
    notifications::feishu::{AlertSender, ReqwestFeishuSender},
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
    pub feishu_alerts: FeishuAlertRepository,
    pub alert_sender: Arc<dyn AlertSender>,
    pub sessions: SessionManager<F>,
}

impl AppState<KafkaSessionClientFactory> {
    // 在服务启动时组装共享仓储、通知发送器和按需会话管理器，不立即连接 Kafka。
    pub fn new(pool: sqlx::SqlitePool) -> Self {
        Self {
            clusters: ClusterRepository::new(pool.clone()),
            lag_rules: LagRuleRepository::new(pool.clone()),
            feishu_alerts: FeishuAlertRepository::new(pool),
            alert_sender: Arc::new(
                ReqwestFeishuSender::new().expect("Failed to initialize notification client"),
            ),
            sessions: SessionManager::new(LEASE_DURATION, KafkaSessionClientFactory),
        }
    }
}

impl<F: SessionClientFactory> AppState<F> {
    // 在测试或定制装配时替换通知发送器，保留其余共享状态。
    pub fn with_alert_sender(mut self, sender: Arc<dyn AlertSender>) -> Self {
        self.alert_sender = sender;
        self
    }

    // 在测试装配时替换会话工厂对应的管理器，使接口复用同一组仓储和发送器。
    pub fn with_session_manager<G: SessionClientFactory>(
        self,
        sessions: SessionManager<G>,
    ) -> AppState<G> {
        AppState {
            clusters: self.clusters,
            lag_rules: self.lag_rules,
            feishu_alerts: self.feishu_alerts,
            alert_sender: self.alert_sender,
            sessions,
        }
    }
}

// 在仅提供 API 时装配已初始化的应用状态，不启用静态页面回退。
pub fn router_with_state<F: SessionClientFactory>(state: AppState<F>) -> Router {
    state_router(state, None)
}

// 在生产服务装配时预先打开并验证静态目录，无有效首页时立即返回启动错误。
pub fn router_with_state_and_web_root<F: SessionClientFactory>(
    state: AppState<F>,
    web_root: Option<PathBuf>,
) -> std::io::Result<Router> {
    // 在接受请求前固定目录能力；后续请求不重新解析可能被替换的配置路径。
    let root = web_root
        .map(|path| static_assets::StaticRoot::new(&path))
        .transpose()?;
    Ok(state_router(state, root))
}

// 在路由构建后注入共享依赖，使各请求复用同一会话管理器及仓储。
fn state_router<F: SessionClientFactory>(
    state: AppState<F>,
    root: Option<static_assets::StaticRoot>,
) -> Router {
    routes::<F>(root)
        .layer(Extension(state.clusters))
        .layer(Extension(state.lag_rules))
        .layer(Extension(state.feishu_alerts))
        .layer(Extension(state.alert_sender))
        .layer(Extension(state.sessions))
}

// 在无需仓储的基础接口测试中构建路由，业务请求仍需调用方注入依赖。
pub fn router() -> Router {
    routes::<KafkaSessionClientFactory>(None)
}

// 在应用装配时注册 API 和受控回退，并统一施加请求大小限制及 Request ID。
fn routes<F: SessionClientFactory>(web_root: Option<static_assets::StaticRoot>) -> Router {
    let router = Router::new()
        .route("/api/health", get(health::health))
        .route("/api/clusters", get(clusters::list).post(clusters::create))
        .route(
            "/api/clusters/test",
            post(clusters::test_draft_connection::<F>),
        )
        .route(
            "/api/clusters/{id}/test",
            post(clusters::test_connection::<F>),
        )
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
        .route(
            "/api/clusters/{id}/feishu-alert",
            get(feishu_alerts::get).put(feishu_alerts::put),
        )
        .route(
            "/api/clusters/{id}/feishu-alert/test",
            post(feishu_alerts::test),
        )
        .route("/api/clusters/{id}/sessions", post(sessions::create::<F>))
        .route("/api/sessions/{id}", delete(sessions::delete::<F>))
        .route("/api/sessions/{id}/snapshot", get(sessions::snapshot::<F>))
        .route("/api/sessions/{id}/topics", post(topics::create::<F>))
        .route(
            "/api/sessions/{id}/topics/{topic}/configs",
            get(topics::configs::<F>).put(topics::update::<F>),
        )
        .route(
            "/api/sessions/{id}/topics/{topic}",
            delete(topics::delete::<F>),
        );
    let router = match web_root {
        Some(root) => router.fallback(move |request| static_assets::serve(root.clone(), request)),
        None => router.fallback(error::not_found),
    };
    router
        .method_not_allowed_fallback(error::method_not_allowed)
        // 先在提取请求体时限制为 256 KiB，防止大体积凭据或 JSON 占用无界内存。
        .layer(DefaultBodyLimit::max(256 * 1024))
        // 最外层先分配 Request ID，使校验、请求体拒绝和安全错误都能关联请求而无需记录正文。
        .layer(middleware::from_fn(error::request_id))
}
