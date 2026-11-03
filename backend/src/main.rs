// 在进程入口同步加载必需的 .env，再初始化日志和异步运行时；版本查询提前返回。
fn main() -> std::io::Result<()> {
    if std::env::args_os()
        .nth(1)
        .is_some_and(|arg| arg == "--version")
    {
        println!("kafka-dashboard {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    // 配置读取或解析失败时统一生成固定错误，避免原始环境值随底层诊断泄漏。
    let invalid_dotenv = || std::io::Error::other("failed to load required .env configuration");
    let dotenv = std::fs::read(".env").map_err(|_| invalid_dotenv())?;
    // 先拒绝 NUL，避免 set_var 的 panic 把配置值带入诊断输出。
    if dotenv.contains(&0) {
        return Err(invalid_dotenv());
    }
    // 迭代解析器不会移除 UTF-8 BOM，因此解析前先兼容文件头。
    let dotenv = dotenv.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(&dotenv);
    for item in dotenvy::from_read_iter(dotenv) {
        let (key, value) = item.map_err(|_| invalid_dotenv())?;
        if std::env::var_os(&key).is_none() {
            // SAFETY: 此同步入口目前只检查参数、读文件和解析配置，均未启动线程。
            // 所有环境写入结束后才初始化日志、Tokio、SQLx 和 Kafka，避免并发修改环境。
            unsafe { std::env::set_var(key, value) };
        }
    }
    tracing_subscriber::fmt::init();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

// 在运行时就绪后初始化数据库与后台监控，启动 HTTP 服务并统一排空退出资源。
async fn run() -> std::io::Result<()> {
    let config = kafka_dashboard::config::Config::from_env()
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let pool = kafka_dashboard::db::connect(&config.database_url)
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let state = kafka_dashboard::api::AppState::new(pool);
    let sessions = state.sessions.clone();
    let monitor = kafka_dashboard::alerts::monitor::start(
        state.clusters.clone(),
        state.feishu_alerts.clone(),
        kafka_dashboard::sessions::manager::KafkaSessionClientFactory,
        state.alert_sender.clone(),
        kafka_dashboard::alerts::monitor::MONITOR_INTERVAL,
    );
    // 所有可能失败的服务装配步骤置于此作用域，使启动错误也走相同的监控和会话排空。
    let result = async {
        let app = kafka_dashboard::api::router_with_state_and_web_root(state, config.web_dir)?;
        let address =
            std::env::var("KAFKA_DASHBOARD_BIND").unwrap_or_else(|_| "127.0.0.1:3000".into());
        let listener = tokio::net::TcpListener::bind(address).await?;
        tracing::info!(address = %listener.local_addr()?, "Kafka Lens listening");
        let shutdown_monitor = monitor.clone();
        let shutdown_sessions = sessions.clone();
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                shutdown_signal().await;
                // 收到 SIGINT/SIGTERM 后先停止后台监控并等待采集及已接受通知，再排空浏览器会话。
                shutdown_monitor.shutdown().await;
                shutdown_sessions.close_all().await;
            })
            .await
    }
    .await;
    // 服务或装配错误可能绕过信号分支，仍须等待监控和会话释放，避免原生工作遗留到后台。
    monitor.shutdown().await;
    sessions.close_all().await;
    result
}

// 在优雅退出流程中等待 SIGINT 或 SIGTERM，让服务有机会释放在途任务及原生资源。
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
