Kafka Lens — offline release

Supported systems: macOS Apple Silicon (Darwin/arm64), Linux x86-64 (Linux/x86_64).
Copy/extract the entire kafka-dashboard directory to a writable location.
No Node.js, Rust, system librdkafka, or system OpenSSL installation is required.
Linux uses musl with statically linked librdkafka and vendored OpenSSL.
macOS uses native system libraries (including libSystem); it is not fully static.

Before first use, check files: shasum -a 256 -c SHA256SUMS
On Linux with coreutils: sha256sum -c SHA256SUMS

Run without any required environment configuration:
  ./start.sh
Open http://127.0.0.1:3000 . Health endpoint: /api/health
Optional bind address: export KAFKA_DASHBOARD_BIND=127.0.0.1:3000
start.sh passes all arguments to the binary; configuration uses environment variables.
To print the binary version, run ./start.sh --version.

.env.example contains optional configuration examples only. start.sh never reads .env files.
The launcher locates web/ and data/ relative to itself even after moving this directory.
It creates data/ with private permissions and stores SQLite at data/kafka-dashboard.db.
DATABASE_URL and KAFKA_DASHBOARD_WEB_DIR can override these defaults.
The working directory is the release directory; explicit relative paths resolve there.
Send SIGTERM or Ctrl-C to shut down gracefully.

WARNING: SQLite stores Kafka passwords, certificates, private keys and private-key
passphrases in PLAINTEXT. Restrict access to the database, sidecars and backups;
API responses and logs must never reveal these credentials.
When upgrading from an encrypted-credentials release, stop the service, manually
delete the old SQLite database and its -wal, -shm or -journal sidecars, then restart
and configure all clusters and Lag rules again. There is no automatic conversion
or deletion. If DATABASE_URL is set, remove that configured database instead.
Never distribute data/, real .env files, credentials, or private keys in a release.

The application has no built-in authentication: use a protected network or an
authenticated HTTPS reverse proxy. Kafka credentials should have read-only permissions.
For private TLS trust, configure the cluster CA PEM explicitly; Linux static OpenSSL
does not bundle your host's trust store. DNS/network access and writable storage remain
runtime requirements. macOS may require approval under your local Gatekeeper policy.
