#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    let config = kafka_dashboard::config::Config::from_env().map_err(std::io::Error::other)?;
    let pool = kafka_dashboard::db::connect(&config.database_url)
        .await
        .map_err(|_| std::io::Error::other("failed to initialize configuration database"))?;
    let app = kafka_dashboard::api::router_with_state(kafka_dashboard::api::AppState::new(
        pool,
        config.cipher,
    ));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    tracing::info!(address = %listener.local_addr()?, "Kafka Lens listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install Ctrl+C handler");
        })
        .await
}
