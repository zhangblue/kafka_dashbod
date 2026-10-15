#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"
run_dir="$(mktemp -d "${TMPDIR:-/tmp}/kafka-dashboard-e2e.XXXXXX")"
project="kafka-dashboard-e2e-$(date +%s)-$$"
backend_pid=''
frontend_pid=''
compose=(docker compose -f "$repo_dir/docker-compose.test.yml" -p "$project")

stop_process() {
  local pid="$1"
  [[ -n "$pid" ]] || return 0
  kill -TERM "$pid" 2>/dev/null || true
  for ((attempt=0; attempt<100; attempt++)); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.1
  done
  if kill -0 "$pid" 2>/dev/null; then
    kill -KILL "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    return 1
  fi
  local exit_code=0
  wait "$pid" 2>/dev/null || exit_code=$?
  # Vite uses Node's normal SIGTERM exit (143); the Rust server must exit 0.
  if [[ "${2:-}" == allow_signal && "$exit_code" == 143 ]]; then return 0; fi
  return "$exit_code"
}

check_logs() {
  E2E_LOG_DIR="$run_dir" node --input-type=module <<'JS'
import fs from 'node:fs'
import path from 'node:path'
const dir = process.env.E2E_LOG_DIR
const secrets = [process.env.E2E_FIXTURE_SECRET].filter(Boolean)
for (const file of fs.readdirSync(dir).filter(name => name.endsWith('.log'))) {
  const log = fs.readFileSync(path.join(dir, file), 'utf8')
  if (secrets.some(secret => log.includes(secret))) {
    console.error('Secret found in captured log; output suppressed')
    process.exit(1)
  }
}
JS
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  stop_process "$frontend_pid" allow_signal || status=1
  stop_process "$backend_pid" || status=1
  "${compose[@]}" down --volumes --remove-orphans >"$run_dir/cleanup.log" 2>&1 || status=1
  check_logs || status=1
  # Only this mktemp-created directory is removed; no external data is touched.
  node -e 'require("node:fs").rmSync(process.argv[1], {recursive:true, force:true})' "$run_dir"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

free_port() {
  node --input-type=module -e 'import net from "node:net"; const s=net.createServer(); s.listen(0,"127.0.0.1",()=>{ console.log(s.address().port); s.close() })'
}

export KAFKA_TEST_PORT="$(free_port)"
export E2E_KAFKA_BROKERS="127.0.0.1:$KAFKA_TEST_PORT"
export E2E_KAFKA_WRITE_PERMIT=disposable-compose
export E2E_RUN_ID="$(node -e 'console.log(require("node:crypto").randomBytes(24).toString("hex"))')"
export E2E_FIXTURE_SECRET="$(node -e 'console.log(require("node:crypto").randomBytes(24).toString("hex"))')"
export DATABASE_URL="sqlite://$run_dir/dashboard.db"
export KAFKA_DASHBOARD_BIND=127.0.0.1:0
export RUST_LOG=info
export NO_COLOR=1
unset FORCE_COLOR

printf '%s\n' 'Starting disposable Kafka fixture…'
"${compose[@]}" up --wait --wait-timeout 180
cargo test -p backend --test kafka_fixture seed_disposable_kafka -- --ignored --exact
cargo build -p backend --bin kafka-dashboard
touch "$run_dir/.env"
(
  cd "$run_dir"
  exec "$repo_dir/target/debug/kafka-dashboard"
) >"$run_dir/backend.log" 2>&1 &
backend_pid=$!
ready=false
for ((attempt=0; attempt<200; attempt++)); do
  kill -0 "$backend_pid" 2>/dev/null || { printf '%s\n' 'Backend failed to start (logs suppressed)'; exit 1; }
  address="$(sed -nE 's/.*address=([^ ]+).*/\1/p' "$run_dir/backend.log" | head -1)"
  if [[ -n "$address" ]] && curl --silent --fail "http://$address/api/health" >/dev/null; then
    kill -0 "$backend_pid" 2>/dev/null || { printf '%s\n' 'Backend exited during readiness'; exit 1; }
    ready=true; break
  fi
  sleep 0.1
done
[[ "$ready" == true ]] || { printf '%s\n' 'Backend readiness timed out'; exit 1; }
export KAFKA_DASHBOARD_API_TARGET="http://$address"
npm --prefix frontend run build
node scripts/e2e-preview.mjs start "$run_dir/preview.json" >"$run_dir/frontend.log" 2>&1 &
frontend_pid=$!
ready=false
for ((attempt=0; attempt<200; attempt++)); do
  kill -0 "$frontend_pid" 2>/dev/null || { printf '%s\n' 'Frontend failed to start (logs suppressed)'; exit 1; }
  if [[ -f "$run_dir/preview.json" ]]; then ready=true; break; fi
  sleep 0.1
done
[[ "$ready" == true ]] || { printf '%s\n' 'Frontend readiness timed out'; exit 1; }
E2E_BASE_URL="$(node scripts/e2e-preview.mjs verify "$run_dir/preview.json" "$frontend_pid")"
export E2E_BASE_URL
kill -0 "$backend_pid" 2>/dev/null || { printf '%s\n' 'Backend exited before browser tests'; exit 1; }
(cd frontend && node node_modules/@playwright/test/cli.js test "$@")
# Assert SIGTERM returns a successful process status before general trap cleanup.
stop_process "$backend_pid"
backend_pid=''
check_logs
printf '%s\n' 'E2E passed; graceful backend shutdown and log secrecy verified.'
