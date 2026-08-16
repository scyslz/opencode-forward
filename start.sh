#!/usr/bin/env bash
# ============================================================
# opencode-zen-proxy 启动脚本
#   支持后台常驻 + 崩溃自动重启 (一直启动)
#   默认启动【两个端口】: 一个 IPv4 出口 + 一个 IPv6 出口 (可 DUAL=0 关掉 ip6)
#   默认转发到 https://opencode.ai/zen/v1
#   自动注入完整 opencode CLI 特征头 + 会话缓存(按 IP家族 映射/落盘)
#
# 用法:
#   ./start.sh [start|stop|status|restart|run [port mode]]
#     默认无参数 = start
#     start 默认起双实例:
#         IPv4 出口 -> 端口 $PORT        (默认 9000)   pid: opencode-zen-proxy.pid
#         IPv6 出口 -> 端口 $PORT_IP6    (默认 9001)   pid: opencode-zen-proxy-ip6.pid
#     run [port mode]   前台运行单实例(带自动重启循环, 适合调试)
#         ./start.sh run                  # 等价 run $PORT 4
#         ./start.sh run 9002 6           # 单起一个 IPv6 出口实例
#
# 环境变量(可选, 都有默认值):
#   PORT                IPv4 出口监听端口  默认 9000
#   PORT_IP6            IPv6 出口监听端口  默认 9001
#   DUAL                1=默认起双实例(ip4+ip6); 0=只起 ip4  默认 1
#   MODE                单实例/调试用出口模式 4/6              默认 4
#   BACKEND             转发目标           默认 https://opencode.ai/zen/v1
#   AUTH                授权token         默认 public (-> Authorization: Bearer public)
#   INBOUND_AUTH        入站客户端校验token 默认空=关闭 (客户端需带 Bearer, 401否则)
#   USER_AGENT          User-Agent        默认 opencode/1.15.0 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.13
#   X_OPENCODE_CLIENT   x-opencode-client 默认 cli (固定)
#   X_OPENCODE_PROJECT  x-opencode-project 默认 global (一般固定)
#   DUMP                1 则开启 --dump (打印每条完整请求 header+body)  默认 0
#   EXTRA_ARGS          额外参数          默认为空(如 --header "X-Api-Key: k")
#   LOG_DIR             日志目录          默认 ./logs
#
# 会话缓存:
#   x-opencode-session 由 proxy 自动 映射/缓存 (落盘到 $LOG_DIR/session-cache.json),
#   按 出口IP家族(4/6) 隔离: 同一原始 session 在 ip4/ip6 出口映射为不同 ses_xxx,
#   切换 ip4/ip6 会话互不串号 (核心)。两个实例共享同一缓存文件, 各自读写自己的命名空间。
#   如需清零缓存: rm -f $LOG_DIR/session-cache.json
# ============================================================
set -euo pipefail

SELF="$0"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$DIR/opencode-zen-proxy"

# 默认配置
PORT="${PORT:-9000}"
PORT_IP6="${PORT_IP6:-9001}"
DUAL="${DUAL:-1}"
MODE="${MODE:-4}"
BACKEND="${BACKEND:-https://opencode.ai/zen/v1}"
AUTH="${AUTH:-public}"
INBOUND_AUTH="${INBOUND_AUTH:-}"
USER_AGENT="${USER_AGENT:-opencode/1.15.0 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.13}"
X_OPENCODE_CLIENT="${X_OPENCODE_CLIENT:-cli}"
X_OPENCODE_PROJECT="${X_OPENCODE_PROJECT:-global}"
DUMP="${DUMP:-0}"
EXTRA_ARGS="${EXTRA_ARGS:-}"
LOG_DIR="${LOG_DIR:-$DIR/logs}"
LOG_FILE="$LOG_DIR/opencode-zen-proxy.log"
PIDFILE="${PIDFILE:-$DIR/opencode-zen-proxy.pid}"
PIDFILE_IP6="$DIR/opencode-zen-proxy-ip6.pid"
CACHE_FILE="${CACHE_FILE:-$LOG_DIR/session-cache.json}"

log() { echo "[$(date '+%F %T')] $*" >> "$LOG_FILE"; }

# build_args: 组装 opencode-zen-proxy 命令行公共参数
build_args() {
    local -a a=(--verbose --cache-file "$CACHE_FILE")
    [ -n "$AUTH" ] && a+=(--auth "$AUTH")
    [ -n "$INBOUND_AUTH" ] && a+=(--inbound-auth "$INBOUND_AUTH")
    [ -n "$USER_AGENT" ] && a+=(--header "User-Agent: $USER_AGENT")
    [ -n "$X_OPENCODE_CLIENT" ] && a+=(--header "x-opencode-client: $X_OPENCODE_CLIENT")
    [ -n "$X_OPENCODE_PROJECT" ] && a+=(--header "x-opencode-project: $X_OPENCODE_PROJECT")
    [ "$DUMP" = "1" ] && a+=(--dump)
    eval "a+=($EXTRA_ARGS)"
    # 以换行分隔输出, worker 用 mapfile 读回, 避免空参数丢失
    printf '%s\n' "${a[@]}"
}

# worker <port> <mode>: 一直运行, 退出/崩溃后自动拉起
worker() {
    local port="$1" mode="$2"
    mkdir -p "$LOG_DIR"
    local -a args=()
    mapfile -t args < <(build_args)
    echo "=== worker 启动: 端口=$port 出口=IPv$mode backend=$BACKEND ==="
    log "worker 启动: 端口=$port 出口=IPv$mode backend=$BACKEND cache=$CACHE_FILE"
    while true; do
        # 用 if 包裹, 避免 set -e 在进程异常退出时把 worker 一起杀掉
        if "$BIN" "$port" "$BACKEND" "$mode" "${args[@]}" >> "$LOG_FILE" 2>&1; then
            log "端口$port 进程退出(退出码0), 3秒后重启"
        else
            local rc=$?
            log "端口$port 进程异常退出(退出码=$rc), 3秒后重启"
        fi
        sleep 3
    done
}

is_running() {
    [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null
}

# run <port> <mode> <pidfile>: 前台跑单个实例的常驻循环
run_one() {
    local port="$1" mode="$2" pidfile="$3"
    if [ ! -f "$pidfile" ]; then
        echo $$ > "$pidfile"
    fi
    worker "$port" "$mode"
}

# spawn <port> <mode> <pidfile>: 拉一个 setsid 独立进程组运行 run_one
spawn() {
    local port="$1" mode="$2" pidfile="$3"
    setsid nohup "$SELF" run "$port" "$mode" >/dev/null 2>&1 &
    echo $! > "$pidfile"
}

start() {
    # IPv4 出口实例
    if is_running "$PIDFILE"; then
        echo "IPv4 已在运行 (pid $(cat "$PIDFILE"))"
    else
        spawn "$PORT" 4 "$PIDFILE"
        echo "IPv4 已启动 (pid $(cat "$PIDFILE"))  端口 $PORT  出口 IPv4"
    fi
    # IPv6 出口实例 (DUAL=1 默认; DUAL=0 跳过)
    if [ "$DUAL" = "1" ]; then
        if is_running "$PIDFILE_IP6"; then
            echo "IPv6 已在运行 (pid $(cat "$PIDFILE_IP6"))"
        else
            spawn "$PORT_IP6" 6 "$PIDFILE_IP6"
            echo "IPv6 已启动 (pid $(cat "$PIDFILE_IP6"))  端口 $PORT_IP6  出口 IPv6"
        fi
    else
        rm -f "$PIDFILE_IP6"
        echo "IPv6 已跳过 (DUAL=0)"
    fi
    echo "后端: $BACKEND   授权: Bearer $AUTH   DUMP=$DUMP"
    echo "入站校验: $([ -n "$INBOUND_AUTH" ] && echo "开启 Bearer $INBOUND_AUTH" || echo "关闭(默认)")"
    echo "会话缓存: $CACHE_FILE   日志: $LOG_FILE"
}

stop_one() {
    local pidfile="$1" label="$2"
    if ! is_running "$pidfile"; then
        rm -f "$pidfile"
        return 1
    fi
    local pid
    pid="$(cat "$pidfile")"
    kill -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 20); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.2
    done
    rm -f "$pidfile"
    echo "$label 已停止"
    return 0
}

stop() {
    local ran=0
    stop_one "$PIDFILE" "IPv4" && ran=1
    stop_one "$PIDFILE_IP6" "IPv6" && ran=1
    [ "$ran" = "1" ] || echo "未在运行"
}

status() {
    if is_running "$PIDFILE"; then
        echo "IPv4 运行中 (pid $(cat "$PIDFILE"))  端口 $PORT  出口 IPv4 -> $BACKEND"
    else
        echo "IPv4 未在运行 (端口 $PORT)"
    fi
    if is_running "$PIDFILE_IP6"; then
        echo "IPv6 运行中 (pid $(cat "$PIDFILE_IP6"))  端口 $PORT_IP6  出口 IPv6 -> $BACKEND"
    else
        echo "IPv6 未在运行 (端口 $PORT_IP6)  $([ "$DUAL" = "1" ] && echo '[DUAL=1 应已启动]' || echo '[DUAL=0 已跳过]')"
    fi
}

restart() { stop; sleep 1; start; }

case "${1:-start}" in
    start)  start ;;
    stop)   stop ;;
    restart) restart ;;
    status) status ;;
    run)
        # ./start.sh run [port mode]
        if [ $# -ge 3 ]; then
            run_one "$2" "$3" "$PIDFILE"
        elif [ $# -eq 2 ]; then
            run_one "$2" "$MODE" "$PIDFILE"
        else
            run_one "$PORT" "$MODE" "$PIDFILE"
        fi
        ;;
    *)
        echo "用法: $0 [start|stop|restart|status|run [port mode]]"
        echo "  start   默认起双实例: IPv4(端口 $PORT) + IPv6(端口 $PORT_IP6)   DUAL=0 只起 ip4"
        echo "  run [port mode]  前台单实例调试, 如: run 9002 6"
        echo "  环境变量: PORT PORT_IP6 DUAL MODE BACKEND AUTH INBOUND_AUTH USER_AGENT"
        echo "            X_OPENCODE_CLIENT X_OPENCODE_PROJECT DUMP"
        echo "            CACHE_FILE LOG_DIR EXTRA_ARGS"
        echo "  (x-opencode-session 由 proxy 按 IP家族 映射/缓存并落盘; x-opencode-request 每次自动生成)"
        exit 1
        ;;
esac