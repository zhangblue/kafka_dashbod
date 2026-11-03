#!/bin/sh
set -eu
export LC_ALL=C
# 参数、来源或产物检查失败时立即终止构建，交给已安装的清理流程恢复现场。
fail() { echo "$*" >&2; exit 1; }
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
cd "$root"
platform=
requested_version=
requested_version_set=0
while [ "$#" -gt 0 ]; do
    case "$1" in
        --platform) [ "$#" -ge 2 ] || fail 'Missing platform'; [ -z "$platform" ] || fail 'Duplicate platform'; platform=$2; shift 2 ;;
        --version) [ "$#" -ge 2 ] || fail 'Missing version'; [ "$requested_version_set" = 0 ] || fail 'Duplicate version'; requested_version_set=1; requested_version=$2; shift 2 ;;
        *) fail 'Usage: tools/build-release.sh --platform linux-amd64|linux-arm64|darwin-arm64 [--version vX.Y.Z]' ;;
    esac
done
# 强制显式选择单个平台，macOS 额外限制本机架构，避免误打包其他目标。
case "$platform" in
    linux-amd64) target=x86_64-unknown-linux-musl; builder=zigbuild ;;
    linux-arm64) target=aarch64-unknown-linux-musl; builder=zigbuild ;;
    darwin-arm64) target=aarch64-apple-darwin; builder=build
        [ "$(uname -s)/$(uname -m)" = Darwin/arm64 ] || fail 'Darwin build host must be Darwin/arm64' ;;
    *) fail 'An explicit supported --platform is required (linux-amd64, linux-arm64, darwin-arm64)' ;;
esac
version=$(sed -n '/^\[package\]/,/^\[/s/^version = "\([0-9][0-9.]*\)"$/\1/p' backend/Cargo.toml)
printf '%s\n' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' || fail 'Invalid Cargo package version'
[ "$requested_version_set" = 0 ] || [ "$requested_version" = "v$version" ] || fail 'Explicit version must match Cargo package version'
# 记录构建前的精确提交和未忽略源码状态；正式发布还要求包内 dirty 为 false。
commit=$(git rev-parse HEAD)
printf '%s\n' "$commit" | grep -Eq '^[0-9a-f]{40}$' || fail 'Invalid source commit'
dirty=false
[ -z "$(git status --porcelain --untracked-files=normal)" ] || dirty=true
name="kafka-dashboard-v$version-$platform"
# 替换、回滚和清理目录前检查 data/.env（包括符号链接），保护已部署的运行数据。
has_runtime() {
    [ -e "$1/data" ] || [ -L "$1/data" ] || [ -e "$1/.env" ] || [ -L "$1/.env" ]
}
# Never replace an unpacked deployment that has acquired runtime state.
! has_runtime "$root/dist/$name" || fail 'Refusing to replace a release containing runtime data or .env'
mkdir -p "$root/dist"
lock="$root/dist/.build-release.lock"
# 同一 worktree 的前端输出与 dist 共用独占锁，编译前拒绝并发构建，避免互相覆盖。
mkdir "$lock" 2>/dev/null || fail 'Another release build is already running'
stage=
publishing=0
complete=0
preserve_stage=0
# 构建退出或信号触发时回滚未完成的三件产物；保留新增运行数据及其备份，最后释放锁。
cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    if [ "$publishing" = 1 ] && [ "$complete" = 0 ]; then
        for suffix in '' .tar.gz .tar.gz.sha256; do
            destination="$root/dist/$name$suffix"
            backup="$stage/previous$name$suffix"
            if [ -e "$backup" ]; then
                if [ -d "$destination" ] && has_runtime "$destination"; then
                    preserve_stage=1
                    echo "Runtime data appeared during rollback; preserving destination and backup: $stage" >&2
                    status=1
                else
                    rm -rf "$destination"
                    mv "$backup" "$destination"
                fi
            elif [ ! -e "$stage/$name$suffix" ]; then
                if [ -d "$destination" ] && has_runtime "$destination"; then
                    echo "Runtime data appeared during rollback; preserving $destination" >&2
                    status=1
                else
                    rm -rf "$destination"
                fi
            fi
        done
    fi
    if [ -n "$stage" ]; then
        # Backups may still be the working directory of a concurrently started
        # service. Never recursively clean a backup that has acquired state.
        if has_runtime "$stage/previous$name"; then preserve_stage=1; fi
        if [ "$preserve_stage" = 0 ]; then rm -rf "$stage"; else echo "Preserved release recovery directory: $stage" >&2; fi
    fi
    rmdir "$lock"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
# 临时构建目录位于 dist 同一文件系统，完成校验后才移动正式产物，失败可回滚。
stage=$(mktemp -d "$root/dist/.release.XXXXXX")
# Compare actual tracked diffs and untracked file contents, not just dirty=true.
# These private files may contain source diffs and are never logged or packaged.
# 编译前后分别记录 HEAD、差异及未跟踪文件内容，防止包内来源信息遗漏构建期间的改动。
snapshot_source() {
    prefix=$1
    git rev-parse HEAD > "$prefix.head"
    git status --porcelain --untracked-files=normal > "$prefix.status"
    git diff --binary HEAD -- > "$prefix.diff"
    git ls-files --others --exclude-standard > "$prefix.untracked"
    git hash-object --stdin-paths < "$prefix.untracked" > "$prefix.contents"
}
# 在安装依赖和编译前建立源码基线；私有差异快照既不打印也不放入归档。
snapshot_source "$stage/source-before"
dirty=false
[ ! -s "$stage/source-before.status" ] || dirty=true
[ "$(cat "$stage/source-before.head")" = "$commit" ] || fail 'Source changed during release build'
# Cargo serializes shared target directories and guarantees successful build output.
# Reuse native dependency caches, including an explicitly supplied target directory.
build_target=${CARGO_TARGET_DIR:-$root/target}
if [ "$builder" = zigbuild ]; then
    # librdkafka 2.12 includes curl headers even when its optional OIDC feature is
    # disabled. Copy only curl/, never a host SDK's libc headers, into the cache.
    curl_include=${KAFKA_DASHBOARD_CURL_INCLUDE_DIR:-}
    if [ -z "$curl_include" ] && command -v xcrun >/dev/null 2>&1; then
        sdk=$(xcrun --show-sdk-path)
        [ ! -f "$sdk/usr/include/curl/curl.h" ] || curl_include="$sdk/usr/include"
    fi
    if [ -z "$curl_include" ]; then
        for include in /usr/include /usr/include/x86_64-linux-gnu /usr/include/aarch64-linux-gnu; do
            if [ -f "$include/curl/curl.h" ]; then curl_include=$include; break; fi
        done
    fi
    [ -n "$curl_include" ] && [ -f "$curl_include/curl/curl.h" ] || fail 'curl headers required: install curl development headers or set KAFKA_DASHBOARD_CURL_INCLUDE_DIR to their include parent'
    mkdir -p "$build_target"
    build_target=$(CDPATH= cd -- "$build_target" && pwd -P)
    headers="$build_target/release-curl-headers"
    mkdir -p "$headers/curl"
    cp -R "$curl_include/curl/." "$headers/curl/"
    flags_key="CFLAGS_$(printf '%s' "$target" | tr '-' '_')"
    existing_flags=$(printenv "$flags_key" || :)
fi
# 锁内先安装并构建前端，再编译唯一目标后端，确保归档静态资源属于本次构建。
npm --prefix frontend ci
npm --prefix frontend run build
if [ "$builder" = zigbuild ]; then
    env "$flags_key=${existing_flags:-${CFLAGS:-}} -I\"$headers\"" cargo "$builder" --locked --release --target "$target" -p backend --bin kafka-dashboard --target-dir "$build_target"
else
    cargo "$builder" --locked --release --target "$target" -p backend --bin kafka-dashboard --target-dir "$build_target"
fi
release="$stage/$name"
mkdir -p "$release/bin" "$release/web"
cp "$build_target/$target/release/kafka-dashboard" "$release/bin/kafka-dashboard-$platform"
cp -R frontend/dist/. "$release/web/"
cp packaging/start.sh packaging/.env.example packaging/README.txt "$release/"
printf '{"version":"%s","platform":"%s","target":"%s","commit":"%s","dirty":%s}\n' "$version" "$platform" "$target" "$commit" "$dirty" > "$release/RELEASE.json"
chmod 755 "$release/start.sh" "$release/bin/"*
(
    cd "$release"
    # 先对包内全部普通文件生成有序完整清单，再压缩，供解包后的完整性核验使用。
    find . -type f ! -name SHA256SUMS | sort | while IFS= read -r file; do shasum -a 256 "$file"; done > SHA256SUMS
)
# 归档完成后再对真实压缩字节生成 sidecar，不能用包内文件哈希代替归档哈希。
COPYFILE_DISABLE=1 tar -czf "$stage/$name.tar.gz" -C "$stage" "$name"
(cd "$stage"; shasum -a 256 "$name.tar.gz" > "$name.tar.gz.sha256")
# 三件候选产物准备完成后重拍源码，全部快照一致才允许替换正式目录。
snapshot_source "$stage/source-after"
for component in head status diff untracked contents; do
    cmp -s "$stage/source-before.$component" "$stage/source-after.$component" || fail 'Source changed during release build'
done
# 先标记可回滚阶段，再逐件备份旧产物并替换；目录移动后重查运行数据，收窄并发部署窗口。
publishing=1
for suffix in '' .tar.gz .tar.gz.sha256; do
    if [ -e "$root/dist/$name$suffix" ]; then mv "$root/dist/$name$suffix" "$stage/previous$name$suffix"; fi
    if [ -z "$suffix" ]; then
        ! has_runtime "$stage/previous$name" || fail 'Refusing to replace a release containing runtime data or .env'
    fi
    mv "$stage/$name$suffix" "$root/dist/$name$suffix"
done
! has_runtime "$stage/previous$name" || fail 'Runtime data appeared during release publication'
complete=1
printf 'Release: %s\nArchive: %s\nChecksum: %s\n' "$root/dist/$name" "$root/dist/$name.tar.gz" "$root/dist/$name.tar.gz.sha256"
