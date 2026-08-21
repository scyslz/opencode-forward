#!/usr/bin/env bash
# ============================================================
# opencode-zen-proxy 统一启动脚本 (单口双栈 6→4, 失败标30s不可用, 双栈失败再集群)
#
# 用法:
#   ./start.sh [start|start-listen|start-join|stop|status|restart|run]
#
# 子命令:
#   start         业务节点 (无集群)
#   start-listen  公网监听节点 (需 CLUSTER_LISTEN)
#   start-join    私网加入节点 (需 CLUSTER_JOIN)
#
# 环境变量:
#   PORT                监听端口              默认 9000
#   BACKEND             转发目标              默认 https://opencode.ai/zen/v1
#   OUTBOUND_AUTH       转发给后端 Bearer     默认空=透传
#   INBOUND_AUTH        入站校验 Bearer       默认空=关闭
#   FWD_INBOUND         1=用INBOUND转发       默认 0
#   EGRESS_PREFER       6/4/auto              默认 6
#   CLUSTER_TOKEN       集群鉴权token
#   CLUSTER_LISTEN      公网监听端口 如 :9443  (start-listen 必填)
#   CLUSTER_JOIN        私网加入公网 如 公网:9443 (start-join 必填)
#   PEERS               逗号分隔对等节点 如 a:9443,b:9443
#   FAILOVER_ON         触发集群转发的状态码/超时 如 429,502,503,504,timeout
#   IP_INTERVAL/IP_URL  出口IP探测
#   USER_AGENT/X_OPENCODE_CLIENT/X_OPENCODE_PROJECT  特征头
#   EXTRA_ARGS          透传参数
# ============================================================
set -euo pipefail

SELF="$0"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$DIR/opencode-zen-proxy"

PORT="${PORT:-9000}"
BACKEND="${BACKEND:-https://opencode.ai/zen/v1}"
OUTBOUND_AUTH="${OUTBOUND_AUTH:-}"
INBOUND_AUTH="${INBOUND_AUTH:-}"
FWD_INBOUND="${FWD_INBOUND:-0}"
EGRESS_PREFER="${PREFER:-${EGRESS_PREFER:-6}}"
CLUSTER_TOKEN="${CLUSTER_TOKEN:-}"
CLUSTER_LISTEN="${CLUSTER_LISTEN:-}"
CLUSTER_JOIN="${CLUSTER_JOIN:-}"
PEERS="${PEERS:-}"
FAILOVER_ON="${FAILOVER_ON:-}"
IP_INTERVAL="${IP_INTERVAL:-5m}"
IP_URL="${IP_URL:-}"
EXTRA_ARGS="${EXTRA_ARGS:-}"
LOG_DIR="${LOG_DIR:-$DIR/logs}"
LOG_FILE="$LOG_DIR/opencode-zen-proxy.log"
PIDFILE="${PIDFILE:-$DIR/opencode-zen-proxy.pid}"
CACHE_FILE="${CACHE_FILE:-$LOG_DIR/session-cache.json}"
DUMP="${DUMP:-0}"
USER_AGENT="${USER_AGENT:-opencode/1.15.0 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.13}"
X_OPENCODE_CLIENT="${X_OPENCODE_CLIENT:-cli}"
X_OPENCODE_PROJECT="${X_OPENCODE_PROJECT:-global}"

log() { echo "[$(date '+%F %T')] $*" >> "$LOG_FILE"; }

build_args() {
    local -a a=(--verbose --cache-file "$CACHE_FILE" --egress-prefer "$EGRESS_PREFER")
    [ -n "$OUTBOUND_AUTH" ] && a+=(--outbound-auth "$OUTBOUND_AUTH")
    [ -n "$INBOUND_AUTH" ] && a+=(--inbound-auth "$INBOUND_AUTH")
    [ "$FWD_INBOUND" = "1" ] && a+=(-F)
    [ -n "$CLUSTER_TOKEN" ] && a+=(--cluster-token "$CLUSTER_TOKEN")
    [ -n "$CLUSTER_LISTEN" ] && a+=(--cluster-listen "$CLUSTER_LISTEN")
    [ -n "$CLUSTER_JOIN" ] && a+=(--cluster-join "$CLUSTER_JOIN")
    if [ -n "$PEERS" ]; then
        IFS=',' read -ra __peers <<< "$PEERS"
        for p in "${__peers[@]}"; do
            p="$(echo "$p" | xargs)"
            [ -n "$p" ] && a+=(--peer "$p")
        done
    fi
    [ -n "$FAILOVER_ON" ] && a+=(--failover-on "$FAILOVER_ON")
    [ -n "$USER_AGENT" ] && a+=(--header "User-Agent: $USER_AGENT")
    [ -n "$X_OPENCODE_CLIENT" ] && a+=(--header "x-opencode-client: $X_OPENCODE_CLIENT")
    [ -n "$X_OPENCODE_PROJECT" ] && a+=(--header "x-opencode-project: $X_OPENCODE_PROJECT")
    [ "$DUMP" = "1" ] && a+=(--dump)
    a+=(--ip-interval "$IP_INTERVAL")
    [ -n "$IP_URL" ] && a+=(--ip-url "$IP_URL")
    eval "a+=($EXTRA_ARGS)"
    printf '%s\n' "${a[@]}"
}

worker() {
    mkdir -p "$LOG_DIR"
    local -a args=()
    mapfile -t args < <(build_args)
    echo "=== worker 启动: 端口=$PORT backend=$BACKEND egress-prefer=$EGRESS_PREFER ==="
    log "worker 启动: 端口=$PORT backend=$BACKEND egress-prefer=$EGRESS_PREFER cache=$CACHE_FILE cluster=$CLUSTER_LISTEN/$CLUSTER_JOIN"
    while true; do
        if "$BIN" "$PORT" "$BACKEND" "${args[@]}" >> "$LOG_FILE" 2>&1; then
            log "端口$PORT 进程退出(0), 3秒后重启"
        else
            local rc=$?
            log "端口$PORT 进程异常退出($rc), 3秒后重启"
        fi
        sleep 3
    done
}

is_running() { [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null; }

run_one() {
    if [ ! -f "$PIDFILE" ]; then echo $$ > "$PIDFILE"; fi
    worker
}

spawn() {
    setsid nohup "$SELF" run >/dev/null 2>&1 &
    echo $! > "$PIDFILE"
}

start() {
    if is_running "$PIDFILE"; then
        echo "已在运行 (pid $(cat "$PIDFILE")) 端口 $PORT"
    else
        spawn
        echo "已启动 (pid $(cat "$PIDFILE"))  端口 $PORT  单口双栈 6→4 (失败标30s不可用, 双栈失败再集群)"
    fi
    echo "后端: $BACKEND  egress-prefer=$EGRESS_PREFER  cache=$CACHE_FILE  日志=$LOG_FILE"
    if [ -n "$CLUSTER_LISTEN" ] || [ -n "$CLUSTER_JOIN" ] || [ -n "$PEERS" ]; then
        echo "集群: listen=$CLUSTER_LISTEN join=$CLUSTER_JOIN peers=$PEERS"
    else
        echo "集群: 未启用 (设 CLUSTER_LISTEN/CLUSTER_JOIN/PEERS 启用)"
    fi
}

start_listen() {
    if [ -z "$CLUSTER_LISTEN" ]; then
        echo "错误: start-listen 需设置 CLUSTER_LISTEN, 如 CLUSTER_LISTEN=:9443 ./start.sh start-listen"
        exit 1
    fi
    start
}

start_join() {
    if [ -z "$CLUSTER_JOIN" ]; then
        echo "错误: start-join 需设置 CLUSTER_JOIN, 如 CLUSTER_JOIN=1.2.3.4:9443 ./start.sh start-join"
        exit 1
    fi
    start
}

stop_one() {
    local pidfile="$1"
    if ! is_running "$pidfile"; then rm -f "$pidfile"; return 1; fi
    local pid; pid="$(cat "$pidfile")"
    kill -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 20); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
    rm -f "$pidfile"; echo "已停止"; return 0
}

stop() { stop_one "$PIDFILE" || echo "未在运行"; }
status() {
    if is_running "$PIDFILE"; then echo "运行中 (pid $(cat "$PIDFILE"))  端口 $PORT -> $BACKEND (单口双栈 6→4 +集群)"; else echo "未在运行 (端口 $PORT)"; fi
}
restart() { stop; sleep 1; start; }

case "${1:-start}" in
    start) start ;;
    start-listen) start_listen ;;
    start-join) start_join ;;
    stop) stop ;;
    status) status ;;
    restart) restart ;;
    run) run_one ;;
    *) echo "用法: $0 [start|start-listen|start-join|stop|status|restart|run]"; exit 1 ;;
esac
