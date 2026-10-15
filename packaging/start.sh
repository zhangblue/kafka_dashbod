#!/bin/sh
set -eu

release_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
case "$(uname -s)/$(uname -m)" in
    Darwin/arm64) platform=darwin-arm64 ;;
    Linux/x86_64) platform=linux-amd64 ;;
    *) echo 'Unsupported platform. Supported: Darwin/arm64, Linux/x86_64.' >&2; exit 1 ;;
esac
cd "$release_dir"
umask 077
mkdir -p "$release_dir/data"
# Keep directory names out of the SQLite URI: '?' and '%xx' are URI syntax.
DATABASE_URL=${DATABASE_URL:-'sqlite://data/kafka-dashboard.db?mode=rwc'}
KAFKA_DASHBOARD_WEB_DIR=${KAFKA_DASHBOARD_WEB_DIR:-"$release_dir/web"}
export DATABASE_URL KAFKA_DASHBOARD_WEB_DIR
exec "$release_dir/bin/kafka-dashboard-$platform" "$@"
