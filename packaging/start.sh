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
exec "$release_dir/bin/kafka-dashboard-$platform" "$@"
