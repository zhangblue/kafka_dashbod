#!/bin/sh
set -eu
export LC_ALL=C

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
cd "$root"
if [ "$(uname -s)/$(uname -m)" != Darwin/arm64 ]; then
    echo 'Build host must be Darwin/arm64 (Apple Silicon macOS).' >&2
    exit 1
fi
mkdir -p "$root/dist"
# Same filesystem as destination, so publication uses rename, never a partial copy.
stage=$(mktemp -d "$root/dist/.release.XXXXXX")
publishing=0
complete=0
cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    if [ "$publishing" = 1 ] && [ "$complete" = 0 ]; then
        # Roll back both outputs if either publication fails.
        if [ -d "$stage/previous-directory" ]; then
            [ ! -e "$root/dist/kafka-dashboard" ] || mv "$root/dist/kafka-dashboard" "$stage/failed-directory"
            mv "$stage/previous-directory" "$root/dist/kafka-dashboard"
        elif [ ! -d "$stage/kafka-dashboard" ] && [ -d "$root/dist/kafka-dashboard" ]; then
            mv "$root/dist/kafka-dashboard" "$stage/failed-directory"
        fi
        if [ -f "$stage/previous-archive" ]; then
            mv -f "$stage/previous-archive" "$root/dist/kafka-dashboard.tar.gz"
        elif [ ! -f "$stage/kafka-dashboard.tar.gz" ] && [ -f "$root/dist/kafka-dashboard.tar.gz" ]; then
            mv "$root/dist/kafka-dashboard.tar.gz" "$stage/failed-archive"
        fi
    fi
    rm -rf "$stage"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

npm --prefix frontend ci
npm --prefix frontend run build
cargo build --locked --release --target aarch64-apple-darwin -p backend --bin kafka-dashboard
linux_output="$stage/linux"
docker buildx build --platform linux/amd64 \
    --file packaging/Dockerfile.linux-amd64 \
    --output "type=local,dest=$linux_output" .

release="$stage/kafka-dashboard"
mkdir -p "$release/bin" "$release/web"
cp target/aarch64-apple-darwin/release/kafka-dashboard "$release/bin/kafka-dashboard-darwin-arm64"
cp "$linux_output/kafka-dashboard-linux-amd64" "$release/bin/kafka-dashboard-linux-amd64"
cp -R frontend/dist/. "$release/web/"
cp packaging/start.sh packaging/.env.example packaging/README.txt "$release/"
chmod 755 "$release/start.sh" "$release/bin/"*
(
    cd "$release"
    find . -type f ! -name SHA256SUMS | sort | while IFS= read -r file; do
        shasum -a 256 "$file"
    done > SHA256SUMS
)
# Suppress macOS resource forks and extended attributes in the portable archive.
COPYFILE_DISABLE=1 tar -czf "$stage/kafka-dashboard.tar.gz" -C "$stage" kafka-dashboard
publishing=1
if [ -e "$root/dist/kafka-dashboard" ]; then mv "$root/dist/kafka-dashboard" "$stage/previous-directory"; fi
if [ -e "$root/dist/kafka-dashboard.tar.gz" ]; then mv "$root/dist/kafka-dashboard.tar.gz" "$stage/previous-archive"; fi
mv "$release" "$root/dist/kafka-dashboard"
mv "$stage/kafka-dashboard.tar.gz" "$root/dist/kafka-dashboard.tar.gz"
complete=1
printf 'Release: %s\nArchive: %s\n' "$root/dist/kafka-dashboard" "$root/dist/kafka-dashboard.tar.gz"
