#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    tracing::info!(address = %listener.local_addr()?, "Kafka Lens listening");

    axum::serve(listener, kafka_dashboard::app_for_test())
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install Ctrl+C handler");
        })
        .await
}
