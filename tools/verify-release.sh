#!/bin/sh
set -eu
export LC_ALL=C
fail() { echo "$*" >&2; exit 1; }
[ "$#" = 1 ] || fail 'Usage: tools/verify-release.sh dist/kafka-dashboard'
release=$(CDPATH= cd -- "$1" && pwd -P)
for required in start.sh .env.example README.txt SHA256SUMS bin/kafka-dashboard-darwin-arm64 bin/kafka-dashboard-linux-amd64 web/index.html; do
    [ -f "$release/$required" ] || fail "Missing release file: $required"
done
[ -z "$(find "$release" ! -type f ! -type d -print)" ] || fail 'Unexpected release symlink or special file'
# Strict allowlist also rejects source maps, real .env, databases and dependency trees.
find "$release" -type f | while IFS= read -r entry; do
    relative=${entry#"$release/"}
    case "$relative" in
        start.sh|.env.example|README.txt|SHA256SUMS|bin/kafka-dashboard-darwin-arm64|bin/kafka-dashboard-linux-amd64|web/index.html) ;;
        web/assets/*.js|web/assets/*.css|web/assets/*.svg|web/assets/*.png|web/assets/*.jpg|web/assets/*.woff2|web/vite.svg) ;;
        *) fail "Unexpected release file: $relative" ;;
    esac
done
find "$release" -type d | while IFS= read -r entry; do
    case "${entry#"$release"}" in ''|/bin|/web|/web/assets) ;; *) fail "Unexpected release directory: $entry" ;; esac
done
for runnable in start.sh bin/kafka-dashboard-darwin-arm64 bin/kafka-dashboard-linux-amd64; do
    [ -x "$release/$runnable" ] || fail "Not executable: $runnable"
done

temp=$(mktemp -d "${TMPDIR:-/tmp}/kafka-release-verify.XXXXXX")
mac_pid=
mac_release=
container=
cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    # The launcher returns before the service exits. Recover its PID even when
    # startup failed before the main flow could read it, and always reap our fixture.
    if [ -n "$mac_release" ]; then
        if [ -z "$mac_pid" ] && [ -f "$mac_release/data/kafka-dashboard.pid" ]; then
            mac_pid=$(cat "$mac_release/data/kafka-dashboard.pid")
        fi
        LC_ALL=en_US.UTF-8 "$mac_release/start.sh" stop >/dev/null 2>&1 || :
    fi
    if [ -n "$mac_pid" ]; then
        if kill -0 "$mac_pid" 2>/dev/null; then kill -KILL "$mac_pid" 2>/dev/null || :; fi
        cleanup_attempt=0
        while kill -0 "$mac_pid" 2>/dev/null && [ "$cleanup_attempt" -lt 20 ]; do
            cleanup_attempt=$((cleanup_attempt + 1))
            sleep 0.1
        done
    fi
    if [ -n "$container" ]; then docker rm -f "$container" >/dev/null 2>&1 || :; fi
    rm -rf "$temp"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
(
    cd "$release"
    find . -type f ! -name SHA256SUMS | sort > "$temp/files"
    # Require a complete, unique, sorted manifest; never accept paths outside the package.
    sed -n 's/^[0-9a-f]\{64\}  \(\.\/.*\)$/\1/p' SHA256SUMS > "$temp/manifest"
    cmp -s "$temp/files" "$temp/manifest" || fail 'Invalid checksum manifest file list'
    shasum -a 256 -c SHA256SUMS || fail 'Release checksum verification failed'
)
file "$release/bin/"*
file "$release/bin/kafka-dashboard-darwin-arm64" | grep -Eq 'Mach-O 64-bit executable arm64' || fail 'Expected Mach-O arm64'
file "$release/bin/kafka-dashboard-linux-amd64" | grep -Eq 'ELF 64-bit.*x86-64' || fail 'Expected ELF x86-64'
file "$release/bin/kafka-dashboard-linux-amd64" | grep -Eq 'statically linked|static-pie linked' || fail 'Linux binary must be statically linked'
[ "$(uname -s)/$(uname -m)" = Darwin/arm64 ] || fail 'Full dual-platform verification requires Darwin/arm64 and Docker'
otool -L "$release/bin/kafka-dashboard-darwin-arm64" | sed '1d' | while IFS= read -r dependency; do
    case "$dependency" in *'/usr/lib/'*|*'/System/Library/'*) ;; *) fail "Non-system macOS dependency: $dependency" ;; esac
done
if grep -REl -- '-----BEGIN .*PRIVATE KEY-----|AKIA[0-9A-Z]{16}' "$release/web" "$release/.env.example" >/dev/null; then
    fail 'Potential secret material in release'
fi
"$release/bin/kafka-dashboard-darwin-arm64" --version
runtime_image=busybox:1.37.0
docker run --rm --platform linux/amd64 -v "$release:/release:ro" "$runtime_image" /release/bin/kafka-dashboard-linux-amd64 --version

mac_release="$temp/mac release?%20中文"
linux_release="$temp/linux release?%20中文"
linux_mount='/release?%20 中文'
cp -R "$release" "$mac_release"
cp -R "$release" "$linux_release"
cp "$mac_release/.env.example" "$mac_release/.env"
cp "$linux_release/.env.example" "$linux_release/.env"
unset DATABASE_URL KAFKA_DASHBOARD_WEB_DIR
# ANSI color disabled so the bound ephemeral port can be read without guessing a free port.
# macOS ps escapes Unicode arguments in the C locale, defeating exact identity
# checks after relocation. Keep C for manifest ordering and UTF-8 for the launcher.
LC_ALL=en_US.UTF-8 NO_COLOR=1 RUST_LOG=info KAFKA_DASHBOARD_BIND=127.0.0.1:0 "$mac_release/start.sh" start
mac_pid=$(cat "$mac_release/data/kafka-dashboard.pid")
mac_status=$(LC_ALL=en_US.UTF-8 "$mac_release/start.sh" status) || fail 'macOS running status command failed'
[ "$mac_status" = "Kafka Lens 正在运行，PID: $mac_pid" ] || fail 'macOS status did not report the exact running service'
mac_log="$mac_release/data/kafka-dashboard.log"
attempt=0
mac_port=
while [ "$attempt" -lt 100 ]; do
    kill -0 "$mac_pid" 2>/dev/null || fail 'macOS server exited before startup'
    mac_port=$(sed -n 's/.*address=127\.0\.0\.1:\([0-9]*\).*/\1/p' "$mac_log" | head -1)
    [ -z "$mac_port" ] || break
    attempt=$((attempt + 1))
    sleep 0.1
done
[ -n "$mac_port" ] || fail 'Timed out waiting for macOS listener'

check_http() {
    base=$1
    destination=$2
    attempt=0
    until curl -fsS --max-time 2 "$base/api/health" > "$temp/health" 2>/dev/null; do
        attempt=$((attempt + 1))
        [ "$attempt" -lt 100 ] || fail "Health timeout: $base"
        sleep 0.1
    done
    grep -q '"status":"ok"' "$temp/health" || fail 'Unexpected health response'
    curl -fsS --max-time 5 "$base/" > "$temp/index"
    cmp -s "$temp/index" "$destination/web/index.html" || fail 'Index response differs from bundled frontend'
    curl -fsS --max-time 5 "$base/clusters/release-check/topics" > "$temp/fallback"
    cmp -s "$temp/index" "$temp/fallback" || fail 'SPA fallback failed'
    assets=$(sed -n 's/.*src="\(\/assets\/[^" ]*\)".*/\1/p; s/.*href="\(\/assets\/[^" ]*\)".*/\1/p' "$destination/web/index.html")
    [ -n "$assets" ] || fail 'Frontend references no assets'
    printf '%s\n' "$assets" | while IFS= read -r asset; do
        curl -fsS --max-time 5 "$base$asset" > "$temp/asset"
        cmp -s "$temp/asset" "$destination/web$asset" || fail "Asset mismatch: $asset"
    done
    [ "$(sqlite3 "$destination/data/kafka-dashboard.db" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('clusters', 'lag_rules');")" = 2 ] || fail 'SQLite migrations missing'
}
check_http "http://127.0.0.1:$mac_port" "$mac_release"
LC_ALL=en_US.UTF-8 "$mac_release/start.sh" stop
mac_status=$(LC_ALL=en_US.UTF-8 "$mac_release/start.sh" status) || fail 'macOS stopped status command failed'
[ "$mac_status" = 'Kafka Lens 未运行' ] || fail 'macOS status did not report the exact stopped state'
[ ! -e "$mac_release/data/kafka-dashboard.pid" ] || fail 'macOS PID file survived start.sh stop'
if kill -0 "$mac_pid" 2>/dev/null; then fail 'macOS process survived start.sh stop'; fi
mac_pid=
echo 'macOS: health, SQLite, index, assets, SPA fallback and background lifecycle passed'

container=$(docker run -d --platform linux/amd64 \
    -v "$linux_release:$linux_mount" -p 127.0.0.1::3000 \
    -e KAFKA_DASHBOARD_BIND=0.0.0.0:3000 \
    "$runtime_image" sh -c 'while :; do sleep 3600; done')
docker exec "$container" "$linux_mount/start.sh" start
linux_pid=$(docker exec "$container" cat "$linux_mount/data/kafka-dashboard.pid")
linux_status=$(docker exec "$container" "$linux_mount/start.sh" status)
printf '%s\n' "$linux_status" | grep -q 'Kafka Lens 正在运行，PID:' || fail 'Linux status did not report a running service'
linux_address=$(docker port "$container" 3000/tcp | head -1)
check_http "http://$linux_address" "$linux_release"
docker exec "$container" "$linux_mount/start.sh" stop
linux_status=$(docker exec "$container" "$linux_mount/start.sh" status)
printf '%s\n' "$linux_status" | grep -q 'Kafka Lens 未运行' || fail 'Linux status did not report a stopped service'
docker exec "$container" sh -c '[ ! -e "$1" ] && ! kill -0 "$2" 2>/dev/null' sh \
    "$linux_mount/data/kafka-dashboard.pid" "$linux_pid" || fail 'Linux PID file or process survived start.sh stop'
docker rm -f "$container" >/dev/null
container=
echo 'Linux/amd64: health, SQLite, index, assets, SPA fallback and background lifecycle passed'
echo 'Dual-platform release verification passed'
