#![cfg(unix)]

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// A symlink index must fail with the setting name before opening the database.
#[test]
fn symlink_index_reports_web_dir_before_database_initialization() {
    let web = tempfile::tempdir().unwrap();
    let database = web.path().join("must-not-be-created.db");
    std::fs::write(web.path().join("page.html"), "<main>test</main>").unwrap();
    std::os::unix::fs::symlink("page.html", web.path().join("index.html")).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_kafka-dashboard"))
        .env_remove("KAFKA_DASHBOARD_MASTER_KEY")
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
#[test]
fn configured_listener_serves_health_and_sigterm_exits_successfully() {
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let mut server = Server(
        Command::new(env!("CARGO_BIN_EXE_kafka-dashboard"))
            .env("KAFKA_DASHBOARD_BIND", address.to_string())
            .env("DATABASE_URL", "sqlite::memory:")
            .env_remove("KAFKA_DASHBOARD_MASTER_KEY")
            .env_remove("KAFKA_DASHBOARD_WEB_DIR")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
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
    assert!(
        Command::new("kill")
            .args(["-TERM", &server.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
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
#[tokio::test]
async fn incompatible_database_exits_with_safe_reconfiguration_instruction() {
    let directory = tempfile::tempdir().unwrap();
    let database_url = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("configuration.db").display()
    );
    let pool = kafka_dashboard::db::connect(&database_url).await.unwrap();
    sqlx::query("UPDATE _sqlx_migrations SET checksum = x'00' WHERE version = 1")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let output = Command::new(env!("CARGO_BIN_EXE_kafka-dashboard"))
        .env("DATABASE_URL", database_url)
        .env_remove("KAFKA_DASHBOARD_MASTER_KEY")
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
