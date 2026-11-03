#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"
# 本次验收独占临时目录与 Compose project，清理只针对本次创建的资源。
run_dir="$(mktemp -d "${TMPDIR:-/tmp}/kafka-dashboard-e2e.XXXXXX")"
project="kafka-dashboard-e2e-$(date +%s)-$$"
backend_pid=''
frontend_pid=''
compose=(docker compose -f "$repo_dir/docker-compose.test.yml" -p "$project")

# 验收收尾和异常清理时停止本次记录的子进程；先等 SIGTERM，失败时回收 fixture 并报告失败。
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

# 清理临时日志前及验收结束时检查测试秘密是否泄漏，命中时抑制原文输出。
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

# EXIT/INT/TERM 收尾先回收前后端，再清理一次性 broker，检查日志后移除本次临时目录。
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

# 创建一次性 Kafka 前向回环接口申请临时端口，供 Compose 的 fixture 地址使用。
free_port() {
  node --input-type=module -e 'import net from "node:net"; const s=net.createServer(); s.listen(0,"127.0.0.1",()=>{ console.log(s.address().port); s.close() })'
}

export KAFKA_TEST_PORT="$(free_port)"
export E2E_KAFKA_BROKERS="127.0.0.1:$KAFKA_TEST_PORT"
# 许可只配合本次动态回环 broker 使用，使写入 fixture 无法误用已有集群。
export E2E_KAFKA_FIXTURE_PERMIT=disposable-compose
export E2E_RUN_ID="$(node -e 'console.log(require("node:crypto").randomBytes(24).toString("hex"))')"
export E2E_FIXTURE_SECRET="$(node -e 'console.log(require("node:crypto").randomBytes(24).toString("hex"))')"
export DATABASE_URL="sqlite://$run_dir/dashboard.db"
export KAFKA_DASHBOARD_BIND=127.0.0.1:0
export RUST_LOG=info
export NO_COLOR=1
unset FORCE_COLOR

printf '%s\n' 'Starting disposable Kafka fixture…'
# 先等待本次 Compose broker 可用，再运行明确授权的写入 fixture，之后才启动受测后端。
"${compose[@]}" up --wait --wait-timeout 180
cargo test -p backend --test kafka_fixture -- --ignored
cargo build -p backend --bin kafka-dashboard
touch "$run_dir/.env"
(
  cd "$run_dir"
  # Vite serves the UI; inherited asset paths may be relative to the repository.
  unset KAFKA_DASHBOARD_WEB_DIR
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
# 浏览器操作前核验 preview 的 PID 与本次标识，防止误连同端口上的其他健康服务。
E2E_BASE_URL="$(node scripts/e2e-preview.mjs verify "$run_dir/preview.json" "$frontend_pid")"
export E2E_BASE_URL
kill -0 "$backend_pid" 2>/dev/null || { printf '%s\n' 'Backend exited before browser tests'; exit 1; }
(cd frontend && node --test scripts/token-safety.test.mjs)
(cd frontend && node node_modules/@playwright/test/cli.js test "$@")
# Assert SIGTERM returns a successful process status before general trap cleanup.
# 正常路径先断言后端 SIGTERM 成功退出，再清空 PID，避免清理 trap 掩盖关闭失败。
stop_process "$backend_pid"
backend_pid=''
check_logs
printf '%s\n' 'E2E passed; graceful backend shutdown and log secrecy verified.'
