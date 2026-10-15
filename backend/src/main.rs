#[tokio::main]
async fn main() -> std::io::Result<()> {
    if std::env::args_os()
        .nth(1)
        .is_some_and(|arg| arg == "--version")
    {
        println!("kafka-dashboard {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    let invalid_dotenv = || std::io::Error::other("failed to load required .env configuration");
    let dotenv = std::fs::read(".env").map_err(|_| invalid_dotenv())?;
    // dotenvy passes values to set_var, whose NUL panic would expose configuration values.
    if dotenv.contains(&0) {
        return Err(invalid_dotenv());
    }
    dotenvy::from_read(dotenv.as_slice()).map_err(|_| invalid_dotenv())?;
    tracing_subscriber::fmt::init();

    let config = kafka_dashboard::config::Config::from_env()
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let pool = kafka_dashboard::db::connect(&config.database_url)
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let state = kafka_dashboard::api::AppState::new(pool);
    let sessions = state.sessions.clone();
    let state_cleanup = sessions.clone();
    let app = kafka_dashboard::api::router_with_state_and_web_root(state, config.web_dir)?;

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
