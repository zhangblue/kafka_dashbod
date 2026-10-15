fn main() -> std::io::Result<()> {
    if std::env::args_os()
        .nth(1)
        .is_some_and(|arg| arg == "--version")
    {
        println!("kafka-dashboard {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    let invalid_dotenv = || std::io::Error::other("failed to load required .env configuration");
    let dotenv = std::fs::read(".env").map_err(|_| invalid_dotenv())?;
    // set_var's NUL panic would expose configuration values.
    if dotenv.contains(&0) {
        return Err(invalid_dotenv());
    }
    // Unlike from_read, dotenvy's iterator does not strip the UTF-8 BOM.
    let dotenv = dotenv.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(&dotenv);
    for item in dotenvy::from_read_iter(dotenv) {
        let (key, value) = item.map_err(|_| invalid_dotenv())?;
        if std::env::var_os(&key).is_none() {
            // SAFETY: This synchronous entry point has only inspected arguments, read the
            // file and parsed dotenv, none of which starts threads. Logging, Tokio, SQLx
            // and Kafka initialization happen below, after every environment write.
            unsafe { std::env::set_var(key, value) };
        }
    }
    tracing_subscriber::fmt::init();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> std::io::Result<()> {
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
