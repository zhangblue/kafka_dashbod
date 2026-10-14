Kafka Lens — offline release

Supported systems: macOS Apple Silicon (Darwin/arm64), Linux x86-64 (Linux/x86_64).
Copy/extract the entire kafka-dashboard directory to a writable location.
No Node.js, Rust, system librdkafka, or system OpenSSL installation is required.
Linux uses musl with statically linked librdkafka and vendored OpenSSL.
macOS uses native system libraries (including libSystem); it is not fully static.

Before first use, check files: shasum -a 256 -c SHA256SUMS
On Linux with coreutils: sha256sum -c SHA256SUMS

Generate a key ONCE with `openssl rand -base64 32`, or a secure password manager.
Store the resulting 32-byte Base64 key securely; do not generate a new key on restart.
Export it as KAFKA_DASHBOARD_MASTER_KEY in your shell or service manager, then run:
  ./start.sh
Open http://127.0.0.1:3000 . Health endpoint: /api/health
Optional bind address: export KAFKA_DASHBOARD_BIND=127.0.0.1:3000
start.sh passes all arguments to the binary; configuration uses environment variables.
To print the binary version without a key, run the matching bin/* binary --version.

.env.example contains placeholders only. start.sh never reads .env files.
The launcher locates web/ and data/ relative to itself even after moving this directory.
It creates data/ with private permissions and stores SQLite at data/kafka-dashboard.db.
DATABASE_URL and KAFKA_DASHBOARD_WEB_DIR can override these defaults.
Send SIGTERM or Ctrl-C to shut down gracefully. Back up SQLite and retain its original key.
Never distribute data/, real .env files, credentials, or private keys in a release.

The application has no built-in authentication: use a protected network or an
authenticated HTTPS reverse proxy. Kafka credentials should have read-only permissions.
For private TLS trust, configure the cluster CA PEM explicitly; Linux static OpenSSL
does not bundle your host's trust store. DNS/network access and writable storage remain
runtime requirements. macOS may require approval under your local Gatekeeper policy.
