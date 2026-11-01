#!/bin/sh
set -eu
export LC_ALL=C
fail() { echo "$*" >&2; exit 1; }
[ "$#" = 1 ] || fail 'Usage: tools/verify-release.sh dist/kafka-dashboard-vVERSION-PLATFORM'
release=$(CDPATH= cd -- "$1" && pwd -P)
[ -f "$release/RELEASE.json" ] || fail 'Missing release file: RELEASE.json'
# Build-generated JSON has a deliberately small fixed schema; reject malformed or
# duplicate fields without adding Node/Python requirements to release verification.
metadata=$(cat "$release/RELEASE.json")
printf '%s\n' "$metadata" | grep -Eq '^\{"version":"[0-9]+\.[0-9]+\.[0-9]+","platform":"(darwin-arm64|linux-amd64|linux-arm64)","target":"[a-z0-9_-]+","commit":"[0-9a-f]{40}","dirty":(true|false)\}$' || fail 'Invalid release metadata'
version=$(printf '%s' "$metadata" | sed 's/.*"version":"\([^"]*\)".*/\1/')
platform=$(printf '%s' "$metadata" | sed 's/.*"platform":"\([^"]*\)".*/\1/')
case "$platform" in
    darwin-arm64) target=aarch64-apple-darwin ;;
    linux-amd64) target=x86_64-unknown-linux-musl; docker_platform=linux/amd64 ;;
    linux-arm64) target=aarch64-unknown-linux-musl; docker_platform=linux/arm64 ;;
esac
printf '%s' "$metadata" | grep -Fq "\"target\":\"$target\"" || fail 'Release target does not match platform'
binary="bin/kafka-dashboard-$platform"
for required in start.sh .env.example README.txt SHA256SUMS RELEASE.json "$binary" web/index.html; do
    [ -f "$release/$required" ] || fail "Missing release file: $required"
done
[ -z "$(find "$release" ! -type f ! -type d -print)" ] || fail 'Unexpected release symlink or special file'
# Strict allowlist also rejects source maps, real .env, databases and dependency trees.
find "$release" -type f | while IFS= read -r entry; do
    relative=${entry#"$release/"}
    case "$relative" in
        start.sh|.env.example|README.txt|SHA256SUMS|RELEASE.json|"$binary"|web/index.html) ;;
        web/assets/*.js|web/assets/*.css|web/assets/*.svg|web/assets/*.png|web/assets/*.jpg|web/assets/*.woff2|web/vite.svg) ;;
        *) fail "Unexpected release file: $relative" ;;
    esac
done
find "$release" -type d | while IFS= read -r entry; do
    case "${entry#"$release"}" in ''|/bin|/web|/web/assets) ;; *) fail "Unexpected release directory: $entry" ;; esac
done
for runnable in start.sh "$binary"; do
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
        "$mac_release/start.sh" stop >/dev/null 2>&1 || :
    fi
    if [ -n "$mac_pid" ]; then
        # A cached numeric PID can have been reused. Recheck the private copy's
        # complete identity and exact PID immediately before any forced cleanup.
        case "$mac_release" in
            "$temp"/*)
                if kill -0 "$mac_pid" 2>/dev/null &&
                    cleanup_status=$("$mac_release/start.sh" status 2>/dev/null) &&
                    [ "$cleanup_status" = "Kafka Lens 正在运行，PID: $mac_pid" ]; then
                    kill -KILL "$mac_pid" 2>/dev/null || :
                fi
                ;;
        esac
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
file "$release/$binary"
case "$platform" in
    darwin-arm64)
        file "$release/$binary" | grep -Eq 'Mach-O 64-bit executable arm64' || fail 'Expected Mach-O arm64'
        [ "$(uname -s)/$(uname -m)" = Darwin/arm64 ] || fail 'macOS verification requires Darwin/arm64'
        otool -L "$release/$binary" | sed '1d' | while IFS= read -r dependency; do
            case "$dependency" in *'/usr/lib/'*|*'/System/Library/'*) ;; *) fail "Non-system macOS dependency: $dependency" ;; esac
        done ;;
    linux-*)
        case "$platform" in linux-amd64) architecture='x86-64';; linux-arm64) architecture='ARM aarch64|aarch64';; esac
        file "$release/$binary" | grep -Eq "ELF 64-bit.*($architecture)" || fail 'Unexpected Linux architecture'
        file "$release/$binary" | grep -Eq 'statically linked|static-pie linked' || fail 'Linux binary must be statically linked' ;;
esac
if grep -REl -- '-----BEGIN .*PRIVATE KEY-----|AKIA[0-9A-Z]{16}' "$release/web" "$release/.env.example" >/dev/null; then
    fail 'Potential secret material in release'
fi
runtime_image=busybox:1.37.0
if [ "$platform" = darwin-arm64 ]; then
    actual_version=$("$release/$binary" --version)
else
    actual_version=$(docker run --rm --platform "$docker_platform" -v "$release:/release:ro" "$runtime_image" "/release/$binary" --version)
fi
[ "$actual_version" = "kafka-dashboard $version" ] || fail 'Binary version does not match release metadata'

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
if [ "$platform" = darwin-arm64 ]; then
mac_release="$temp/mac release?%20中文"
cp -R "$release" "$mac_release"
cp "$mac_release/.env.example" "$mac_release/.env"
unset DATABASE_URL KAFKA_DASHBOARD_WEB_DIR
# ANSI color disabled so the bound ephemeral port can be read without guessing a free port.
NO_COLOR=1 RUST_LOG=info KAFKA_DASHBOARD_BIND=127.0.0.1:0 "$mac_release/start.sh" start
mac_pid=$(cat "$mac_release/data/kafka-dashboard.pid")
mac_status=$("$mac_release/start.sh" status) || fail 'macOS running status command failed'
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

check_http "http://127.0.0.1:$mac_port" "$mac_release"
"$mac_release/start.sh" stop
mac_status=$("$mac_release/start.sh" status) || fail 'macOS stopped status command failed'
[ "$mac_status" = 'Kafka Lens 未运行' ] || fail 'macOS status did not report the exact stopped state'
[ ! -e "$mac_release/data/kafka-dashboard.pid" ] || fail 'macOS PID file survived start.sh stop'
if kill -0 "$mac_pid" 2>/dev/null; then fail 'macOS process survived start.sh stop'; fi
mac_pid=
echo 'macOS: health, SQLite, index, assets, SPA fallback and background lifecycle passed'

else
linux_release="$temp/linux release?%20中文"
linux_mount='/release?%20 中文'
cp -R "$release" "$linux_release"
cp "$linux_release/.env.example" "$linux_release/.env"
container=$(docker run -d --platform "$docker_platform" --user "$(id -u):$(id -g)" \
    -v "$linux_release:$linux_mount" -p 127.0.0.1::3000 \
    -e KAFKA_DASHBOARD_BIND=0.0.0.0:3000 \
    "$runtime_image" sh -c 'while :; do sleep 3600; done')
docker exec "$container" "$linux_mount/start.sh" start
linux_pid=$(docker exec "$container" cat "$linux_mount/data/kafka-dashboard.pid")
linux_status=$(docker exec "$container" "$linux_mount/start.sh" status) || fail 'Linux status command failed'
[ "$linux_status" = "Kafka Lens 正在运行，PID: $linux_pid" ] || fail 'Linux status did not report the exact running service'
linux_address=$(docker port "$container" 3000/tcp | head -1)
check_http "http://$linux_address" "$linux_release"
docker exec "$container" "$linux_mount/start.sh" stop
linux_status=$(docker exec "$container" "$linux_mount/start.sh" status) || fail 'Linux status command failed'
[ "$linux_status" = 'Kafka Lens 未运行' ] || fail 'Linux status did not report the exact stopped state'
docker exec "$container" sh -c '[ ! -e "$1" ] && ! kill -0 "$2" 2>/dev/null' sh \
    "$linux_mount/data/kafka-dashboard.pid" "$linux_pid" || fail 'Linux PID file or process survived start.sh stop'
docker rm -f "$container" >/dev/null
container=
printf '%s: health, SQLite, index, assets, SPA fallback and background lifecycle passed\n' "$docker_platform"
fi
printf '%s release verification passed\n' "$platform"
