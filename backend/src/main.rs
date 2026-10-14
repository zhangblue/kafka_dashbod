#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    let config = kafka_dashboard::config::Config::from_env().map_err(std::io::Error::other)?;
    let pool = kafka_dashboard::db::connect(&config.database_url)
        .await
        .map_err(|_| std::io::Error::other("failed to initialize configuration database"))?;
    let state = kafka_dashboard::api::AppState::new(pool, config.cipher);
    let sessions = state.sessions.clone();
    let state_cleanup = sessions.clone();
    let app = kafka_dashboard::api::router_with_state(state);

    let address = std::env::var("KAFKA_DASHBOARD_BIND").unwrap_or_else(|_| "127.0.0.1:3000".into());
    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!(address = %listener.local_addr()?, "Kafka Lens listening");

    let result = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            sessions.close_all().await;
        })
        .await;
    // Also cover server errors that bypass the shutdown signal future.
    state_cleanup.close_all().await;
    result
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    }
}
