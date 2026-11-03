#![cfg(unix)]

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    process::{Child, Command, Output, Stdio},
    time::{Duration, Instant},
};

struct Server(Child);
impl Drop for Server {
    // 在进程测试失败或退出时终止并回收所拥有的子进程，避免遗留进程或僵尸。
    fn drop(&mut self) {
        // 异常兜底先终止本测试拥有的子进程，再 wait 回收；正常 SIGTERM 另由健康断言验证。
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// 在进程测试准备阶段写入临时配置并隔离父环境，返回尚未启动的命令。
fn command_with_dotenv(directory: &std::path::Path, contents: &str) -> Command {
    std::fs::write(directory.join(".env"), contents).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_kafka-dashboard"));
    command
        .current_dir(directory)
        .env_remove("DATABASE_URL")
        .env_remove("KAFKA_DASHBOARD_BIND")
        .env_remove("KAFKA_DASHBOARD_WEB_DIR")
        .env_remove("RUST_LOG");
    command
}

// 在启动失败测试中有界等待真实子进程退出，退出后收集诊断供安全断言。
fn output_before_timeout(command: &mut Command) -> Output {
    let mut server = Server(
        command
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    // 先有界等待退出再读取诊断，避免失败启动无限占用测试。
    let status = loop {
        if let Some(status) = server.0.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "startup did not exit promptly");
        std::thread::sleep(Duration::from_millis(20));
    };
    let mut stderr = Vec::new();
    server
        .0
        .stderr
        .take()
        .unwrap()
        .read_to_end(&mut stderr)
        .unwrap();
    Output {
        status,
        stdout: Vec::new(),
        stderr,
    }
}

// Missing dotenv loading must not allow normal startup to continue.
// 验证普通启动必须读取当前目录的配置文件，缺失时不能继续初始化。
#[test]
fn startup_requires_dotenv_in_current_directory() {
    let directory = tempfile::tempdir().unwrap();
    let output = output_before_timeout(
        Command::new(env!("CARGO_BIN_EXE_kafka-dashboard"))
            .current_dir(directory.path())
            .env_remove("DATABASE_URL")
            .env_remove("KAFKA_DASHBOARD_BIND")
            .env_remove("KAFKA_DASHBOARD_WEB_DIR"),
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("required .env"), "{stderr}");
}

// Version queries must not load dotenv or initialize Tokio, even with invalid runtime settings.
// 验证版本查询不读取配置或初始化运行时，避免查询因环境配置失败。
#[test]
fn version_does_not_require_dotenv() {
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_kafka-dashboard"))
        .current_dir(directory.path())
        .arg("--version")
        .env("TOKIO_WORKER_THREADS", "0")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).starts_with("kafka-dashboard "));
}

// Creating Tokio first would panic on its worker count and bypass the safe dotenv error.
// 验证配置解析先于运行时初始化，非法运行时参数不能遮蔽安全配置错误。
#[test]
fn dotenv_failure_precedes_runtime_initialization() {
    let directory = tempfile::tempdir().unwrap();
    let mut command = command_with_dotenv(directory.path(), "DATABASE_URL='dotenv-secret\n");
    command.env("TOKIO_WORKER_THREADS", "0");
    let output = output_before_timeout(&mut command);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("required .env"), "{stderr}");
    assert!(!stderr.contains("panicked"), "{stderr}");
    assert!(!stderr.contains("dotenv-secret"), "{stderr}");
}

// Raw parser errors must not echo dotenv values in startup diagnostics.
// 验证错误配置格式返回安全启动诊断，不能回显解析输入值。
#[test]
fn malformed_dotenv_exits_without_echoing_values() {
    let directory = tempfile::tempdir().unwrap();
    let mut command = command_with_dotenv(directory.path(), "DATABASE_URL='dotenv-secret\n");
    let output = output_before_timeout(&mut command);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("required .env"), "{stderr}");
    assert!(!stderr.contains("dotenv-secret"), "{stderr}");
}

// Passing a NUL-containing value to set_var panics and exposes its full contents.
// 验证含 NUL 的配置安全失败而非 panic，避免环境赋值诊断泄露内容。
#[test]
fn nul_dotenv_exits_without_echoing_values_or_panicking() {
    let directory = tempfile::tempdir().unwrap();
    let mut command = command_with_dotenv(directory.path(), "DATABASE_URL=nul-secret\0suffix\n");
    let output = output_before_timeout(&mut command);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("nul-secret"),
        "startup diagnostics exposed the configuration value"
    );
    assert!(
        !stderr.contains("panicked"),
        "invalid configuration must return a safe error rather than panic"
    );
    assert!(
        stderr.contains("required .env"),
        "startup diagnostics omitted the safe configuration error"
    );
}

// A symlink index must fail with the setting name before opening the database.
// 验证符号链接首页在打开数据库前被拒绝，且只报告配置项名称。
#[test]
fn symlink_index_reports_web_dir_before_database_initialization() {
    let web = tempfile::tempdir().unwrap();
    let database = web.path().join("must-not-be-created.db");
    std::fs::write(web.path().join("page.html"), "<main>test</main>").unwrap();
    std::os::unix::fs::symlink("page.html", web.path().join("index.html")).unwrap();
    let output = command_with_dotenv(web.path(), "")
        .env("KAFKA_DASHBOARD_WEB_DIR", web.path())
        .env(
            "DATABASE_URL",
            format!("sqlite://{}?mode=rwc", database.display()),
        )
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("KAFKA_DASHBOARD_WEB_DIR"), "{stderr}");
    assert!(!stderr.contains("database"), "{stderr}");
    assert!(
        !database.exists(),
        "invalid web assets must fail before SQLite creation"
    );
}

// Catches ignoring the configured address, and abrupt or hanging SIGTERM shutdown.
// 验证真实进程使用指定监听地址，并在 SIGTERM 后有界正常退出。
#[test]
fn configured_listener_serves_health_and_sigterm_exits_successfully() {
    let directory = tempfile::tempdir().unwrap();
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let dotenv = format!("\u{feff}KAFKA_DASHBOARD_BIND={address}\nDATABASE_URL=sqlite::memory:\n");
    let mut command = command_with_dotenv(directory.path(), &dotenv);
    let server = Server(
        command
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    assert_health_and_sigterm(server, address);
}

// Persisted eligible alerts must start without a browser session. The peer is a
// local inert TCP stub, not a Kafka broker; no metadata or delivery can succeed.
// 验证无浏览器时已存告警仍启动后台连接，SIGTERM 等待其释放；对端仅为本地惰性桩。
#[tokio::test]
async fn persisted_alert_worker_starts_and_sigterm_drains_it() {
    use kafka_dashboard::{
        db::{clusters::ClusterRepository, feishu_alerts::FeishuAlertRepository},
        domain::{cluster::ClusterInput, feishu_alert::FeishuAlertRuleInput},
    };

    let directory = tempfile::tempdir().unwrap();
    let database_url = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("configuration.db").display()
    );
    let peer = TcpListener::bind("127.0.0.1:0").unwrap();
    peer.set_nonblocking(true).unwrap();
    let pool = kafka_dashboard::db::connect(&database_url).await.unwrap();
    let mut input =
        ClusterInput::plaintext("process fixture", peer.local_addr().unwrap().to_string());
    input.connection_timeout_ms = 100;
    let cluster = ClusterRepository::new(pool.clone())
        .create(&input)
        .await
        .unwrap();
    FeishuAlertRepository::new(pool.clone())
        .replace(
            &cluster.id,
            Some("process-test-token"),
            false,
            &[FeishuAlertRuleInput {
                id: None,
                group_id: "process-test-group".into(),
                topic: "process-test-topic".into(),
                lag_threshold: 5_000,
                growth_seconds: 10,
                enabled: true,
            }],
        )
        .await
        .unwrap();
    // 先结束准备阶段的数据库连接，再交给真实进程读取持久化告警配置。
    pool.close().await;

    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let mut server = Server(
        command_with_dotenv(directory.path(), "")
            .env("DATABASE_URL", database_url)
            .env("KAFKA_DASHBOARD_BIND", address.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(3);
    // 先观察后台监控主动建立连接，再验证健康与终止，确保覆盖后台客户端清理。
    let _socket = loop {
        match peer.accept() {
            Ok((socket, _)) => break socket,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("local stub failed: {error}"),
        }
        assert!(
            Instant::now() < deadline,
            "eligible background monitor did not create a Kafka client"
        );
        assert!(
            server.0.try_wait().unwrap().is_none(),
            "server exited before monitor startup"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_health_and_sigterm(server, address);
}

// Override loading would select the invalid file database instead of the external value.
// 验证外部环境变量优先于文件配置，避免加载过程覆盖有效数据库地址。
#[test]
fn external_environment_overrides_dotenv() {
    let directory = tempfile::tempdir().unwrap();
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let server = Server(
        command_with_dotenv(directory.path(), "DATABASE_URL=invalid://from-dotenv\n")
            .env("KAFKA_DASHBOARD_BIND", address.to_string())
            .env("DATABASE_URL", "sqlite::memory:")
            .env_remove("KAFKA_DASHBOARD_WEB_DIR")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    assert_health_and_sigterm(server, address);
}

// env::var treats non-UTF-8 values as absent and would replace the external path with dotenv.
// 验证非 UTF-8 的外部目录值不被当作缺失并由文件覆盖。
#[test]
fn non_utf8_external_web_directory_is_not_replaced_by_dotenv() {
    use std::os::unix::ffi::OsStrExt;

    let directory = tempfile::tempdir().unwrap();
    let web = directory.path().join("web");
    std::fs::create_dir(&web).unwrap();
    std::fs::write(web.join("index.html"), "<main>dotenv web root</main>").unwrap();
    let mut command = command_with_dotenv(
        directory.path(),
        "KAFKA_DASHBOARD_WEB_DIR=web\nDATABASE_URL=sqlite::memory:\nKAFKA_DASHBOARD_BIND=127.0.0.1:0\n",
    );
    // This path is intentionally absent; macOS cannot create non-UTF-8 filenames.
    command.env(
        "KAFKA_DASHBOARD_WEB_DIR",
        std::ffi::OsStr::from_bytes(b"missing-web-\xff"),
    );
    let output = output_before_timeout(&mut command);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("KAFKA_DASHBOARD_WEB_DIR"), "{stderr}");
    assert!(!stderr.contains("required .env"), "{stderr}");
    assert!(!stderr.contains("missing-web"), "{stderr}");
}

// 在进程存活测试中先确认健康响应，再发送终止信号并等待优雅退出。
fn assert_health_and_sigterm(mut server: Server, address: std::net::SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(mut stream) = TcpStream::connect_timeout(&address, Duration::from_millis(100)) {
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            stream
                .write_all(
                    b"GET /api/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            assert!(response.starts_with("HTTP/1.1 200"));
            break;
        }
        assert!(
            Instant::now() < deadline,
            "server did not bind the configured address"
        );
        assert!(
            server.0.try_wait().unwrap().is_none(),
            "server exited before readiness"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    // 健康检查完成后只向本测试子进程发 SIGTERM，检验正常停机而非兜底强制终止。
    assert!(
        Command::new("kill")
            .args(["-TERM", &server.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    // 发送信号后持续回收退出状态，正常退出才证明运行时已完成停机。
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = server.0.try_wait().unwrap() {
            assert!(
                status.success(),
                "SIGTERM must finish graceful shutdown: {status}"
            );
            break;
        }
        assert!(Instant::now() < deadline, "graceful shutdown timed out");
        std::thread::sleep(Duration::from_millis(20));
    }
}

// Migration checksum mismatches require a safe recovery instruction, without SQL details.
// 验证迁移不兼容时提供安全的手动重配提示，不能回显 SQL 或校验细节。
#[tokio::test]
async fn incompatible_database_exits_with_safe_reconfiguration_instruction() {
    let directory = tempfile::tempdir().unwrap();
    let database_url = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("configuration.db").display()
    );
    let pool = kafka_dashboard::db::connect(&database_url).await.unwrap();
    // 只破坏临时数据库的迁移校验和，确定性触发不兼容分支而不接触真实配置。
    sqlx::query("UPDATE _sqlx_migrations SET checksum = x'00' WHERE version = 1")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let output = command_with_dotenv(directory.path(), "")
        .env("DATABASE_URL", database_url)
        .env_remove("KAFKA_DASHBOARD_WEB_DIR")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("configuration database is incompatible"),
        "{stderr}"
    );
    assert!(
        stderr.contains("delete it and configure clusters again"),
        "{stderr}"
    );
    for private_detail in ["checksum", "SELECT", "_sqlx_migrations", "x'00'"] {
        assert!(!stderr.contains(private_detail), "{stderr}");
    }
}

// Ordinary connection failures must not suggest deleting configuration or expose native details.
// 验证普通数据库连接失败仅报告安全初始化错误，不能误导用户删除配置。
#[test]
fn unavailable_database_exits_with_safe_initialization_error() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory
        .path()
        .join("missing-parent/fixture-unavailable.db");
    let output = command_with_dotenv(directory.path(), "")
        .env(
            "DATABASE_URL",
            format!("sqlite://{}?mode=rwc", database.display()),
        )
        .env_remove("KAFKA_DASHBOARD_WEB_DIR")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("failed to initialize configuration database"),
        "{stderr}"
    );
    for private_detail in [
        "configuration database is incompatible",
        "delete it",
        "SELECT",
        "SQLite",
        "sqlite",
        "sqlx",
        "unable to open database file",
        "fixture-unavailable",
        directory.path().to_str().unwrap(),
    ] {
        assert!(!stderr.contains(private_detail), "{stderr}");
    }
}
