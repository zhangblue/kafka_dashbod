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
            .env(
                "KAFKA_DASHBOARD_MASTER_KEY",
                "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=",
            )
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
