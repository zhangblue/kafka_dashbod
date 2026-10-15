#![cfg(unix)]

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    process::{Child, Command, Output, Stdio},
    time::{Duration, Instant},
};

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

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

fn output_before_timeout(command: &mut Command) -> Output {
    let mut server = Server(
        command
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(5);
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

// The version branch must stay before required dotenv loading.
#[test]
fn version_does_not_require_dotenv() {
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_kafka-dashboard"))
        .current_dir(directory.path())
        .arg("--version")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).starts_with("kafka-dashboard "));
}

// Raw parser errors must not echo dotenv values in startup diagnostics.
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

// A symlink index must fail with the setting name before opening the database.
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
#[test]
fn configured_listener_serves_health_and_sigterm_exits_successfully() {
    let directory = tempfile::tempdir().unwrap();
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let dotenv = format!("KAFKA_DASHBOARD_BIND={address}\nDATABASE_URL=sqlite::memory:\n");
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

// Override loading would select the invalid file database instead of the external value.
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
