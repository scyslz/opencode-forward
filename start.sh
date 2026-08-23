#!/usr/bin/env bash
set -euo pipefail

SELF="$0"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$DIR/opencode-zen-proxy"

PORT="${PORT:-9000}"
BACKEND="${BACKEND:-https://opencode.ai/zen/v1}"
OUTBOUND_AUTH="${OUTBOUND_AUTH:-public}"
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
TUNNEL_FILE="${TUNNEL_FILE:-$LOG_DIR/tunnels.json}"
SESSION_FILE="${SESSION_FILE:-$LOG_DIR/session-map.json}"
DUMP="${DUMP:-0}"
USER_AGENT="${USER_AGENT:-opencode/1.15.0 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.13}"
X_OPENCODE_CLIENT="${X_OPENCODE_CLIENT:-cli}"
X_OPENCODE_PROJECT="${X_OPENCODE_PROJECT:-global}"

log() { echo "[$(date '+%F %T')] $*" >> "$LOG_FILE"; }

build_args() {
    local -a a=(--verbose --cache-file "$CACHE_FILE" --egress-prefer "$EGRESS_PREFER" --tunnel-file "$TUNNEL_FILE")
    [ -n "$OUTBOUND_AUTH" ] && a+=(--outbound-auth "$OUTBOUND_AUTH")
    [ -n "$INBOUND_AUTH" ] && a+=(--inbound-auth "$INBOUND_AUTH")
    [ "$FWD_INBOUND" = "1" ] && a+=(-F)
    [ -n "$CLUSTER_TOKEN" ] && a+=(--cluster-token "$CLUSTER_TOKEN")
    [ -n "$CLUSTER_LISTEN" ] && a+=(--cluster-listen "$CLUSTER_LISTEN")
    [ -n "$CLUSTER_JOIN" ] && a+=(--cluster-join "$CLUSTER_JOIN")
    if [ -n "$PEERS" ]; then
        IFS=';' read -ra __peers <<< "$PEERS"
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
    log "worker 启动: 端口=$PORT backend=$BACKEND egress-prefer=$EGRESS_PREFER cache=$CACHE_FILE session-map=$SESSION_FILE cluster=$CLUSTER_LISTEN/$CLUSTER_JOIN"
    "$BIN" "$PORT" "$BACKEND" "${args[@]}" >> "$LOG_FILE" 2>&1
}

is_running() { [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null; }

kill_by_port() {
    local port="$1"
    local pids=""
    pids=$(ss -tlnp "sport = :$port" 2>/dev/null | grep -o 'pid=[0-9]*' | cut -d= -f2 | sort -u 2>/dev/null || true)
    if [ -n "$pids" ]; then
        for pid in $pids; do
            [ "$pid" = "$$" ] && continue
            kill -9 "$pid" 2>/dev/null || true
        done
        sleep 0.3
    fi
    return 0
}

kill_old() {
    local pidfile="${1:-$PIDFILE}"
    if is_running "$pidfile"; then
        local pid; pid="$(cat "$pidfile")"
        kill -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
        for _ in $(seq 1 40); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
        rm -f "$pidfile"
    fi
    kill_by_port "$PORT"
}

start() {
    kill_old "$PIDFILE"
    nohup "$SELF" run >/dev/null 2>&1 &
    local child_pid=$!
    echo "$child_pid" > "$PIDFILE"
    sleep 2
    if kill -0 "$child_pid" 2>/dev/null; then
        echo "已启动 (pid $child_pid)  端口 $PORT  单口双栈 6→4 (失败标30s不可用, 双栈失败再集群)"
    else
        echo "启动失败，检查日志: $LOG_FILE"
        exit 1
    fi
    echo "后端: $BACKEND  egress-prefer=$EGRESS_PREFER  cache=$CACHE_FILE  日志=$LOG_FILE"
    if [ -n "$CLUSTER_LISTEN" ] || [ -n "$CLUSTER_JOIN" ] || [ -n "$PEERS" ]; then
        echo "集群: listen=$CLUSTER_LISTEN join=$CLUSTER_JOIN peers=$PEERS"
    else
        echo "集群: 未启用 (设 CLUSTER_LISTEN/CLUSTER_JOIN/PEERS 启用)"
    fi
}

stop() {
    if ! is_running "$PIDFILE"; then rm -f "$PIDFILE"; echo "未在运行"; return; fi
    local pid; pid="$(cat "$PIDFILE")"
    kill -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 40); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
    rm -f "$PIDFILE"
    echo "已停止"
}

case "${1:-start}" in
    start) start ;;
    stop) stop ;;
    run) worker ;;
    *) echo "用法: $0 [start|stop]"; exit 1 ;;
esac
