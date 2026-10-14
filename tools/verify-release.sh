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
container=
cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    if [ -n "$mac_pid" ]; then
        kill -TERM "$mac_pid" 2>/dev/null || :
        cleanup_attempt=0
        while kill -0 "$mac_pid" 2>/dev/null && [ "$cleanup_attempt" -lt 20 ]; do
            cleanup_attempt=$((cleanup_attempt + 1))
            sleep 0.1
        done
        if kill -0 "$mac_pid" 2>/dev/null; then kill -KILL "$mac_pid" 2>/dev/null || :; fi
        wait "$mac_pid" 2>/dev/null || :
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
grep -qx 'KAFKA_DASHBOARD_MASTER_KEY=REPLACE_WITH_BASE64_ENCODED_32_RANDOM_BYTES' "$release/.env.example" || fail 'Example master key must remain a placeholder'
"$release/bin/kafka-dashboard-darwin-arm64" --version
runtime_image=busybox:1.37.0
docker run --rm --platform linux/amd64 -v "$release:/release:ro" "$runtime_image" /release/bin/kafka-dashboard-linux-amd64 --version

mac_release="$temp/mac release?%20中文"
linux_release="$temp/linux release?%20中文"
linux_mount='/release?%20 中文'
cp -R "$release" "$mac_release"
cp -R "$release" "$linux_release"
KAFKA_DASHBOARD_MASTER_KEY=$(openssl rand -base64 32)
export KAFKA_DASHBOARD_MASTER_KEY
unset DATABASE_URL KAFKA_DASHBOARD_WEB_DIR
# ANSI color disabled so the bound ephemeral port can be read without guessing a free port.
NO_COLOR=1 RUST_LOG=info KAFKA_DASHBOARD_BIND=127.0.0.1:0 "$mac_release/start.sh" > "$temp/mac.log" 2>&1 &
mac_pid=$!
attempt=0
mac_port=
while [ "$attempt" -lt 100 ]; do
    kill -0 "$mac_pid" 2>/dev/null || fail 'macOS server exited before startup'
    mac_port=$(sed -n 's/.*address=127\.0\.0\.1:\([0-9]*\).*/\1/p' "$temp/mac.log" | head -1)
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
kill -TERM "$mac_pid"
attempt=0
while kill -0 "$mac_pid" 2>/dev/null; do
    attempt=$((attempt + 1))
    [ "$attempt" -lt 100 ] || fail 'macOS did not exit after SIGTERM'
    sleep 0.1
done
wait "$mac_pid" || fail 'macOS SIGTERM exit was not clean'
mac_pid=
echo 'macOS: health, SQLite, index, assets, SPA fallback and SIGTERM passed'

container=$(docker run -d --platform linux/amd64 \
    -v "$linux_release:$linux_mount" -p 127.0.0.1::3000 \
    -e KAFKA_DASHBOARD_MASTER_KEY -e KAFKA_DASHBOARD_BIND=0.0.0.0:3000 \
    "$runtime_image" "$linux_mount/start.sh")
linux_address=$(docker port "$container" 3000/tcp | head -1)
check_http "http://$linux_address" "$linux_release"
docker kill --signal TERM "$container" >/dev/null
attempt=0
while [ "$(docker inspect --format '{{.State.Running}}' "$container")" = true ]; do
    attempt=$((attempt + 1))
    [ "$attempt" -lt 100 ] || fail 'Linux did not exit after SIGTERM'
    sleep 0.1
done
[ "$(docker inspect --format '{{.State.ExitCode}}' "$container")" = 0 ] || fail 'Linux SIGTERM exit was not clean'
docker logs "$container" > "$temp/linux.log" 2>&1
if grep -Fq "$KAFKA_DASHBOARD_MASTER_KEY" "$temp/mac.log" "$temp/linux.log"; then fail 'Master key leaked into runtime logs'; fi
docker rm "$container" >/dev/null
container=
echo 'Linux/amd64: health, SQLite, index, assets, SPA fallback and SIGTERM passed'
echo 'Dual-platform release verification passed'
