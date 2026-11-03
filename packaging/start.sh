#!/bin/sh
set -eu

release_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
# 启动入口先按操作系统和架构选择唯一二进制，避免执行其他平台产物。
case "$(uname -s)/$(uname -m)" in
    Darwin/arm64) platform=darwin-arm64 ;;
    Linux/x86_64) platform=linux-amd64 ;;
    Linux/aarch64|Linux/arm64) platform=linux-arm64 ;;
    *) echo 'Unsupported platform. Supported: Darwin/arm64, Linux/x86_64, Linux/aarch64.' >&2; exit 1 ;;
esac
binary="$release_dir/bin/kafka-dashboard-$platform"
data_dir="$release_dir/data"
pid_file="$data_dir/kafka-dashboard.pid"
log_file="$data_dir/kafka-dashboard.log"
lock_dir="$data_dir/kafka-dashboard.lock"

# 参数无效时输出中文子命令用法，供入口校验调用。
usage() { echo '用法: ./start.sh [start|stop|status|restart|--version]' >&2; }
# 管理操作遇到不可安全继续的状态时输出错误并结束启动器。
fail() { echo "$*" >&2; exit 1; }
# 管理操作退出时释放本次持有的空锁目录，由退出 trap 调用。
release_lock() { [ ! -d "$lock_dir" ] || rmdir "$lock_dir" 2>/dev/null || :; }
# 启动、停止、查询和重启前取得独占锁，并为退出及信号路径安装释放处理。
acquire_lock() {
    mkdir "$lock_dir" 2>/dev/null || fail '无法执行操作：另一个启动器管理操作正在进行'
    trap release_lock EXIT
    trap 'exit 129' HUP
    trap 'exit 130' INT
    trap 'exit 143' TERM
}
# 后台子进程创建后以同目录临时文件原子写入 PID，避免其他管理操作读到半条记录。
write_pid() {
    pid_tmp="$pid_file.$$"
    printf '%s\n' "$1" > "$pid_tmp" && mv "$pid_tmp" "$pid_file"
}
# inspect_process 确认 PID 存活后核验内核可执行文件和完整参数边界，禁止仅凭命令显示名发信号。
# macOS 分支内的 latin 读取原始字节，uint 解码小端整数，utf8 解码参数；run 在 osascript 入口组合这些 helper 完成身份核验。
# 参数缓冲含环境数据，核验只返回匹配标志，不能输出缓冲内容。
matches_process_identity() {
    case "$platform" in
        darwin-arm64)
            case "$process_command" in "$binary"|"/bin/sh $binary") ;; *) return 1 ;; esac
            # Read the kernel executable and argc/NUL argv boundaries directly.
            # System JXA bridges libproc/sysctl without GUI automation or installs.
            # Darwin/arm64 uses little-endian int32 and size_t64 for these calls.
            # The argument buffer also contains the environment: never print it.
            identity=$(LC_ALL=en_US.UTF-8 osascript -l JavaScript -e '
                ObjC.import("Foundation");
                ObjC.bindFunction("proc_pidpath", ["int", ["int", "void *", "uint32"]]);
                ObjC.bindFunction("sysctl", ["int", ["void *", "uint32", "void *", "void *", "void *", "uint64"]]);
                function latin(data) {
                    return $.NSString.alloc.initWithDataEncoding(data, $.NSISOLatin1StringEncoding).js;
                }
                function uint(raw, start, count) {
                    var value = 0;
                    for (var i = 0; i < count; i++) value += raw.charCodeAt(start + i) * Math.pow(256, i);
                    return value;
                }
                function utf8(raw) {
                    var data = $(raw).dataUsingEncoding($.NSISOLatin1StringEncoding);
                    return $.NSString.alloc.initWithDataEncoding(data, $.NSUTF8StringEncoding).js;
                }
                function run(argv) {
                    var pid = Number(argv[0]), binary = argv[1];
                    var path = $.NSMutableData.dataWithLength(4096);
                    var count = $.proc_pidpath(pid, path.mutableBytes, 4096);
                    if (count <= 0 || count >= 4096) throw new Error("Unreadable executable");
                    path.length = count;
                    var executable = $.NSString.alloc.initWithDataEncoding(path, $.NSUTF8StringEncoding).js;
                    var encoded = "";
                    [1, 49, pid].forEach(function(value) {
                        for (var i = 0; i < 4; i++) encoded += String.fromCharCode((value >>> (i * 8)) & 255);
                    });
                    var mib = $(encoded).dataUsingEncoding($.NSISOLatin1StringEncoding);
                    var size = $.NSMutableData.dataWithLength(8);
                    if ($.sysctl(mib.bytes, 3, null, size.mutableBytes, null, 0) !== 0) throw new Error("Unreadable arguments");
                    var length = uint(latin(size), 0, 8);
                    if (length < 5 || length > 1048576) throw new Error("Invalid argument size");
                    var buffer = $.NSMutableData.dataWithLength(length);
                    if ($.sysctl(mib.bytes, 3, buffer.mutableBytes, size.mutableBytes, null, 0) !== 0) throw new Error("Unreadable arguments");
                    buffer.length = uint(latin(size), 0, 8);
                    var raw = latin(buffer), argc = uint(raw, 0, 4), end = raw.indexOf("\0", 4);
                    if (argc < 1 || argc > 2 || end < 0) throw new Error("Invalid arguments");
                    var cursor = end;
                    while (cursor < raw.length && raw.charCodeAt(cursor) === 0) cursor++;
                    var args = [];
                    for (var i = 0; i < argc; i++) {
                        end = raw.indexOf("\0", cursor);
                        if (end < 0) throw new Error("Missing argument boundary");
                        args.push(utf8(raw.slice(cursor, end)));
                        cursor = end + 1;
                    }
                    if (executable === binary && argc === 1 && args[0] === binary) return "match";
                    if ((executable === "/bin/sh" || executable === "/bin/bash") &&
                        argc === 2 && args[0] === "/bin/sh" && args[1] === binary) return "match";
                    throw new Error("Identity mismatch");
                }
            ' "$pid" "$binary" 2>/dev/null) || return 1
            [ "$identity" = match ]
            ;;
        linux-amd64|linux-arm64)
            process_executable=$(readlink "/proc/$pid/exe") || return 1
            case "$process_command" in
                "$binary")
                    [ "$process_executable" = "$binary" ] || return 1
                    printf '%s\000' "$binary" | cmp -s - "/proc/$pid/cmdline"
                    ;;
                "/bin/sh $binary")
                    [ "$process_executable" = "$(readlink -f /bin/sh)" ] || return 1
                    printf '%s\000' /bin/sh "$binary" | cmp -s - "/proc/$pid/cmdline"
                    ;;
                "[qemu] $binary $binary")
                    # Emulation must use the same kernel executable as this
                    # launcher, not an unrelated process merely named qemu.
                    launcher_executable=$(readlink "/proc/$$/exe") || return 1
                    [ "$process_executable" = "$launcher_executable" ] || return 1
                    case "$platform:$process_executable" in
                        linux-amd64:*/qemu-x86_64|linux-amd64:*/qemu-x86_64-static|linux-amd64:*/x86_64-binfmt-P|linux-arm64:*/qemu-aarch64|linux-arm64:*/qemu-aarch64-static|linux-arm64:*/aarch64-binfmt-P) ;;
                        *) return 1 ;;
                    esac
                    printf '%s\000' '[qemu]' "$binary" "$binary" | cmp -s - "/proc/$pid/cmdline"
                    ;;
                *) return 1 ;;
            esac
            ;;
    esac
}

# 0: matching live service; 1: stopped; 2: unsafe or unreadable process state.
# 各管理子命令调用时读取并验证 PID；区分服务存活、已停止与不可安全操作的状态。
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
    if [ "$platform" = darwin-arm64 ]; then
        process_table=$(LC_ALL=en_US.UTF-8 ps -e -o pid= -o args=) || {
            echo '无法检查进程' >&2
            return 2
        }
    elif ! process_table=$(ps -e -o pid= -o args=); then
        echo '无法检查进程' >&2
        return 2
    fi
    process_command=$(printf '%s\n' "$process_table" | awk -v target="$pid" '
        $1 == target && !found {
            sub(/^[[:space:]]*[0-9]+[[:space:]]+/, "")
            # BusyBox prefixes shebang commands with a {comm} annotation.
            sub(/^\{[^}]+\}[[:space:]]+/, "")
            command = $0
            found = 1
        }
        END { if (found) print command }')
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
    # 存活检测不足以证明所有权，必须在返回可管理状态前核验完整进程身份。
    if matches_process_identity; then return 0; fi
    echo 'PID 文件对应的进程身份不匹配' >&2
    return 2
}
# start 或成功停止后的 restart 调用；先拒绝重复服务，再后台启动并确认初始存活。
start_service() {
    if inspect_process; then
        echo "Kafka Lens 已在运行，PID: $pid" >&2
        return 1
    else
        state=$?
        [ "$state" -eq 1 ] || return "$state"
    fi
    # 进程检查确认为停止后才创建子进程；日志追加保留既有诊断记录。
    nohup "$binary" </dev/null >>"$log_file" 2>&1 &
    service_pid=$!
    if ! write_pid "$service_pid"; then
        # PID 记录失败时先终止并等待刚创建的子进程，避免产生无法管理的后台服务。
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
# status 子命令调用时输出运行或停止状态；两种业务状态均成功返回，检查错误保持非零。
status_service() {
    if inspect_process; then
        echo "Kafka Lens 正在运行，PID: $pid"
    else
        state=$?
        [ "$state" -eq 1 ] || return "$state"
        echo 'Kafka Lens 未运行'
    fi
}
# stop 或 restart 调用；仅向身份匹配的服务发送 SIGTERM，并有界等待正常释放资源。
stop_service() {
    if inspect_process; then
        service_pid=$pid
    else
        state=$?
        [ "$state" -eq 1 ] || return "$state"
        echo 'Kafka Lens 未运行'
        return 0
    fi
    # 完整身份核验后才发送 SIGTERM；超时保留 PID，不升级为 SIGKILL，以便继续安全管理。
    kill -TERM "$service_pid" || fail "无法停止 Kafka Lens，PID: $service_pid"
    attempt=0
    while kill -0 "$service_pid" 2>/dev/null; do
        attempt=$((attempt + 1))
        [ "$attempt" -lt 100 ] || fail "Kafka Lens 在等待时间内未退出，PID 文件已保留: $pid_file"
        sleep 0.1 || fail "无法等待 Kafka Lens 退出，PID 文件已保留: $pid_file"
    done
    rm -f "$pid_file" || fail '无法删除 PID 文件'
    echo 'Kafka Lens 已停止'
}
# restart 子命令先等待停止成功再启动，停止失败时保留现有进程与记录。
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
# 所有管理操作共用一把锁，防止 PID 检查、写入与停止互相交错。
acquire_lock
case "$command" in
    start) start_service ;;
    status) status_service ;;
    stop) stop_service ;;
    restart) restart_service ;;
esac
