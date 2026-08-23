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

VERBOSE="${VERBOSE:-0}"
LOG_MAX_MB="${LOG_MAX_MB:-20}"
LOG_MAX_FILES="${LOG_MAX_FILES:-5}"
LOG_LEVEL="${LOG_LEVEL:-info}"
LEVEL_NUM() { case "${1:-info}" in debug) echo 0;; info) echo 1;; warn) echo 2;; error) echo 3;; *) echo 1;; esac; }
CURRENT_LEVEL="$(LEVEL_NUM "$LOG_LEVEL")"
log() { echo "[$(date '+%F %T')] $*" >> "$LOG_FILE"; }
rotate_logs() {
    [ -f "$LOG_FILE" ] || return 0
    local sz; sz=$(stat -c%s "$LOG_FILE" 2>/dev/null || stat -f%z "$LOG_FILE" 2>/dev/null || echo 0)
    local max=$(( LOG_MAX_MB * 1024 * 1024 ))
    if [ "$sz" -gt "$max" ]; then
        for i in $(seq $((LOG_MAX_FILES-1)) -1 1); do [ -f "$LOG_FILE.$i" ] && mv -f "$LOG_FILE.$i" "$LOG_FILE.$((i+1))"; done
        mv -f "$LOG_FILE" "$LOG_FILE.1"
        : > "$LOG_FILE"
        echo "[$(date '+%F %T')] 日志轮转: 已归档 $LOG_FILE.1 (阈值 ${LOG_MAX_MB}MB, 保留 ${LOG_MAX_FILES} 份)" >> "$LOG_FILE"
    fi
}
trim_args_for_log() {
    local out="" tok
    for tok in "$@"; do
        case "$tok" in
            --cluster-token|--outbound-auth|--inbound-auth) out+=" $tok ****" ;;
            *) case "$tok" in *" "*|*"'"*|*'"'*) out+=" '$tok'" ;; *) out+=" $tok" ;; esac ;;
        esac
    done
    echo "$out"
}

build_args() {
    local -a a=(--cache-file "$CACHE_FILE" --egress-prefer "$EGRESS_PREFER" --tunnel-file "$TUNNEL_FILE")
    [ "$VERBOSE" = "1" ] && a+=(--verbose)
    [ "$LOG_LEVEL" != "info" ] && a+=(--log-level "$LOG_LEVEL")
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
    rotate_logs
    local -a args=()
    mapfile -t args < <(build_args)
    local safe_args; safe_args=$(trim_args_for_log "${args[@]}")
    echo "[$(date '+%F %T')] worker 启动: $BIN $PORT $BACKEND $safe_args" >> "$LOG_FILE"
    log "worker 启动: 端口=$PORT backend=$BACKEND egress-prefer=$EGRESS_PREFER cache=$CACHE_FILE session-map=$SESSION_FILE cluster=$CLUSTER_LISTEN/$CLUSTER_JOIN level=$LOG_LEVEL verbose=$VERBOSE rotate=${LOG_MAX_MB}MBx${LOG_MAX_FILES}"
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

egress_human() {
    case "$EGRESS_PREFER" in
        d4) echo "强制 IPv4" ;;
        d6) echo "强制 IPv6" ;;
        4)  echo "优先 IPv4,失败回退IPv6" ;;
        6)  echo "优先 IPv6,失败回退IPv4" ;;
        auto) echo "并发竞速" ;;
        *) echo "$EGRESS_PREFER" ;;
    esac
}
start() {
    kill_old "$PIDFILE"
    nohup "$SELF" run >/dev/null 2>&1 &
    local child_pid=$!
    echo "$child_pid" > "$PIDFILE"
    sleep 2
    if kill -0 "$child_pid" 2>/dev/null; then
        if [ "$VERBOSE" = "1" ]; then echo "已启动 (pid $child_pid)  日志 VERBOSE=1/debug (含每请求)"; else echo "已启动 (pid $child_pid)  日志 $LOG_LEVEL 精简"; fi
        echo "监听 :$PORT 单端口双栈  后端 $BACKEND"
        echo "出口 $(egress_human)  冷却 30s  探测 $IP_INTERVAL  集群 ${CLUSTER_JOIN:-${CLUSTER_LISTEN:-未启用}}"
    else
        echo "启动失败，检查日志: $LOG_FILE"
        exit 1
    fi
    echo "日志 $LOG_FILE  轮转 ${LOG_MAX_MB}MBx${LOG_MAX_FILES}"
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
