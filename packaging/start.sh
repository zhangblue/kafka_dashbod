#!/bin/sh
set -eu

release_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
case "$(uname -s)/$(uname -m)" in
    Darwin/arm64) platform=darwin-arm64 ;;
    Linux/x86_64) platform=linux-amd64 ;;
    *) echo 'Unsupported platform. Supported: Darwin/arm64, Linux/x86_64.' >&2; exit 1 ;;
esac
binary="$release_dir/bin/kafka-dashboard-$platform"
data_dir="$release_dir/data"
pid_file="$data_dir/kafka-dashboard.pid"
log_file="$data_dir/kafka-dashboard.log"
lock_dir="$data_dir/kafka-dashboard.lock"

usage() { echo '用法: ./start.sh [start|stop|status|restart|--version]' >&2; }
fail() { echo "$*" >&2; exit 1; }
release_lock() { [ ! -d "$lock_dir" ] || rmdir "$lock_dir" 2>/dev/null || :; }
acquire_lock() {
    mkdir "$lock_dir" 2>/dev/null || fail '无法执行操作：另一个启动器管理操作正在进行'
    trap release_lock EXIT
    trap 'exit 129' HUP
    trap 'exit 130' INT
    trap 'exit 143' TERM
}
write_pid() {
    pid_tmp="$pid_file.$$"
    printf '%s\n' "$1" > "$pid_tmp" && mv "$pid_tmp" "$pid_file"
}

# 0: matching live service; 1: stopped; 2: unsafe or unreadable process state.
inspect_process() {
    if [ ! -e "$pid_file" ] && [ ! -L "$pid_file" ]; then return 1; fi
    if [ ! -f "$pid_file" ] || [ ! -r "$pid_file" ]; then
        echo '无法读取 PID 文件' >&2
        return 2
    fi
    if ! pid=$(cat "$pid_file"); then
        echo '无法读取 PID 文件' >&2
        return 2
    fi
    case "$pid" in
        ''|*[!0-9]*|0*) echo 'PID 文件内容无效' >&2; return 2 ;;
    esac
    if ! process_table=$(ps -e -o pid= -o args=); then
        echo '无法检查进程' >&2
        return 2
    fi
    process_command=$(printf '%s\n' "$process_table" | awk -v target="$pid" '
        $1 == target {
            sub(/^[[:space:]]*[0-9]+[[:space:]]+/, "")
            # BusyBox prefixes shebang commands with a {comm} annotation.
            sub(/^\{[^}]+\}[[:space:]]+/, "")
            print
            exit
        }')
    if ! kill -0 "$pid" 2>/dev/null; then
        if [ -n "$process_command" ]; then
            echo '无法检查进程：没有访问权限' >&2
            return 2
        fi
        if ! rm -f "$pid_file"; then
            echo '无法清理陈旧 PID 文件' >&2
            return 2
        fi
        return 1
    fi
    if [ -z "$process_command" ]; then
        echo '无法检查进程：命令不可读' >&2
        return 2
    fi
    # This launcher never passes service arguments. Match the complete command,
    # including the direct POSIX-shell interpreter form used by script binaries.
    case "$process_command" in
        "$binary"|"/bin/sh $binary") return 0 ;;
        *) echo 'PID 文件对应的进程身份不匹配' >&2; return 2 ;;
    esac
}
start_service() {
    if inspect_process; then
        echo "Kafka Lens 已在运行，PID: $pid" >&2
        return 1
    else
        state=$?
        [ "$state" -eq 1 ] || return "$state"
    fi
    nohup "$binary" </dev/null >>"$log_file" 2>&1 &
    service_pid=$!
    if ! write_pid "$service_pid"; then
        kill -TERM "$service_pid" 2>/dev/null || :
        wait "$service_pid" 2>/dev/null || :
        rm -f "$pid_tmp"
        fail '无法写入 PID 文件'
    fi
    sleep 1
    if ! kill -0 "$service_pid" 2>/dev/null; then
        rm -f "$pid_file"
        fail "Kafka Lens 启动失败，请查看日志: $log_file"
    fi
    echo "Kafka Lens 已在后台启动，PID: $service_pid"
    echo "日志文件: $log_file"
}
status_service() {
    if inspect_process; then
        echo "Kafka Lens 正在运行，PID: $pid"
    else
        state=$?
        [ "$state" -eq 1 ] || return "$state"
        echo 'Kafka Lens 未运行'
    fi
}
stop_service() {
    if inspect_process; then
        service_pid=$pid
    else
        state=$?
        [ "$state" -eq 1 ] || return "$state"
        echo 'Kafka Lens 未运行'
        return 0
    fi
    kill -TERM "$service_pid" || fail "无法停止 Kafka Lens，PID: $service_pid"
    attempt=0
    while kill -0 "$service_pid" 2>/dev/null; do
        attempt=$((attempt + 1))
        [ "$attempt" -lt 100 ] || fail "Kafka Lens 在等待时间内未退出，PID 文件已保留: $pid_file"
        sleep 0.1
    done
    rm -f "$pid_file" || fail '无法删除 PID 文件'
    echo 'Kafka Lens 已停止'
}
restart_service() {
    stop_service || return "$?"
    start_service
}

[ "$#" -le 1 ] || { usage; exit 1; }
command=${1-start}
case "$command" in
    --version) exec "$binary" --version ;;
    start|stop|status|restart) ;;
    *) usage; exit 1 ;;
esac
cd "$release_dir"
umask 077
mkdir -p "$data_dir"
acquire_lock
case "$command" in
    start) start_service ;;
    status) status_service ;;
    stop) stop_service ;;
    restart) restart_service ;;
esac
