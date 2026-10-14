#!/bin/sh
set -eu

release_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
case "$(uname -s)/$(uname -m)" in
    Darwin/arm64) platform=darwin-arm64 ;;
    Linux/x86_64) platform=linux-amd64 ;;
    *) echo 'Unsupported platform. Supported: Darwin/arm64, Linux/x86_64.' >&2; exit 1 ;;
esac
if [ -z "${KAFKA_DASHBOARD_MASTER_KEY:-}" ]; then
    echo 'KAFKA_DASHBOARD_MASTER_KEY is required (32 random bytes encoded as Base64).' >&2
    exit 1
fi
umask 077
mkdir -p "$release_dir/data"
DATABASE_URL=${DATABASE_URL:-"sqlite://$release_dir/data/kafka-dashboard.db?mode=rwc"}
KAFKA_DASHBOARD_WEB_DIR=${KAFKA_DASHBOARD_WEB_DIR:-"$release_dir/web"}
export DATABASE_URL KAFKA_DASHBOARD_WEB_DIR
exec "$release_dir/bin/kafka-dashboard-$platform" "$@"
