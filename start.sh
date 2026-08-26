#!/usr/bin/env bash
# opencode-zen-proxy 服务管理脚本
# 用法: ./start.sh [start|stop|restart|status|run]
#   start   后台启动(默认): 日志写入文件
#   run     前台启动: 日志实时输出到终端(同时落盘)
#   stop    停止
#   restart 重启
#   status  查看状态
# 交互式: 在终端下运行且环境变量未设置时会提问; 管道/cron 等非交互环境自动用默认值
set -u

SELF="$(readlink -f "$0")"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$DIR/opencode-zen-proxy"

BACKEND="${BACKEND:-https://opencode.ai/zen/v1}"
OUTBOUND_AUTH="${OUTBOUND_AUTH:-public}"
FWD_INBOUND="${FWD_INBOUND:-0}"
EGRESS_PREFER="${PREFER:-${EGRESS_PREFER:-6}}"
CLUSTER_LISTEN="${CLUSTER_LISTEN:-}"
CLUSTER_JOIN="${CLUSTER_JOIN:-}"
PEERS="${PEERS:-}"
FAILOVER_ON="${FAILOVER_ON:-429,502,503,504,timeout}"
IP_INTERVAL="${IP_INTERVAL:-5m}"
IP_URL="${IP_URL:-}"
DUMP="${DUMP:-0}"
VERBOSE="${VERBOSE:-0}"
USER_AGENT="${USER_AGENT:-opencode/1.18.23 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14}"
X_OPENCODE_CLIENT="${X_OPENCODE_CLIENT:-cli}"
X_OPENCODE_PROJECT="${X_OPENCODE_PROJECT:-global}"
LOG_DIR="${LOG_DIR:-$DIR/logs}"
LOG_MAX_MB="${LOG_MAX_MB:-20}"
LOG_MAX_FILES="${LOG_MAX_FILES:-5}"

PORT="${PORT:-9000}"
INBOUND_AUTH="${INBOUND_AUTH:-}"
DNS_SERVER="${DNS_SERVER:-}"
MODEL="${MODEL:-}"
CLUSTER_TOKEN="${CLUSTER_TOKEN:-}"
EXTRA_ARGS="${EXTRA_ARGS:-}"

# ---- 交互式: 仅当 stdin 是终端时对未设置的项提问 ----
ask() { # $1=变量名 $2=提示 $3=默认值 -> stdout
    local env="$1" p="$2" d="$3" v=""
    eval "v=\"\${$env:-}\""
    [ -n "$v" ] && { echo "$v"; return; }
    if [ -t 0 ]; then
        printf '%s [%s]: ' "$p" "$d"
        read -r v || v=""
        echo "${v:-$d}"
    else
        echo "$d"
    fi
}
if [ -t 0 ]; then
    echo "== opencode-zen-proxy 配置 (回车=默认) =="
    PORT="$(ask PORT '本地监听端口' '9000')"
    INBOUND_AUTH="$(ask INBOUND_AUTH '入站鉴权 token' '')"
    EGRESS_PREFER="$(ask EGRESS_PREFER '出口优先(6/4/d4/d6/auto)' '6')"
    DNS_SERVER="$(ask DNS_SERVER 'DNS服务器' '')"
    MODEL="$(ask MODEL '模型替换' '')"
fi

LOG_FILE="$LOG_DIR/opencode-zen-proxy.log"
PIDFILE="${PIDFILE:-$DIR/opencode-zen-proxy-$PORT.pid}"
CACHE_FILE="${CACHE_FILE:-$LOG_DIR/session-cache.json}"

log() { echo "[$(date '+%F %T')] $*" >> "$LOG_FILE"; }

rotate_logs() {
    [ -f "$LOG_FILE" ] || return 0
    local sz max=$(( LOG_MAX_MB * 1024 * 1024 )) i
    sz=$(stat -c%s "$LOG_FILE" 2>/dev/null || echo 0)
    [ "$sz" -le "$max" ] && return 0
    for ((i=LOG_MAX_FILES-1; i>=1; i--)); do
        [ -f "$LOG_FILE.$i" ] && mv -f "$LOG_FILE.$i" "$LOG_FILE.$((i+1))"
    done
    mv -f "$LOG_FILE" "$LOG_FILE.1"
    : > "$LOG_FILE"
    log "日志轮转: 归档 $LOG_FILE.1"
}

trim_args() {
    local out="" tok
    for tok in "$@"; do
        case "$tok" in
            --cluster-token|--outbound-auth|--inbound-auth) out+=" $tok ****" ;;
            *" "*|*"'"*|*'"'*) out+=" '$tok'" ;;
            *) out+=" $tok" ;;
        esac
    done
    echo "$out"
}

build_args() {
    local -a a=(--cache-file "$CACHE_FILE" --egress-prefer "$EGRESS_PREFER")
    [ "$VERBOSE" = "1" ]        && a+=(--verbose)
    [ -n "$OUTBOUND_AUTH" ]     && a+=(--outbound-auth "$OUTBOUND_AUTH")
    [ -n "$INBOUND_AUTH" ]      && a+=(--inbound-auth "$INBOUND_AUTH")
    [ "$FWD_INBOUND" = "1" ]    && a+=(-F)
    [ -n "$MODEL" ]             && a+=(--model "$MODEL")
    [ -n "$DNS_SERVER" ]        && a+=(--dns-server "$DNS_SERVER")
    [ -n "$CLUSTER_TOKEN" ]     && a+=(--cluster-token "$CLUSTER_TOKEN")
    [ -n "$CLUSTER_LISTEN" ]    && a+=(--cluster-listen "$CLUSTER_LISTEN")
    [ -n "$CLUSTER_JOIN" ]      && a+=(--cluster-join "$CLUSTER_JOIN")
    if [ -n "$PEERS" ]; then
        local p
        IFS=';' read -ra __peers <<< "$PEERS"
        for p in "${__peers[@]}"; do
            p="$(echo "$p" | xargs)"
            [ -n "$p" ] && a+=(--peer "$p")
        done
    fi
    [ -n "$FAILOVER_ON" ]       && a+=(--failover-on "$FAILOVER_ON")
    [ -n "$USER_AGENT" ]        && a+=(--header "User-Agent: $USER_AGENT")
    [ -n "$X_OPENCODE_CLIENT" ] && a+=(--header "x-opencode-client: $X_OPENCODE_CLIENT")
    [ -n "$X_OPENCODE_PROJECT" ]&& a+=(--header "x-opencode-project: $X_OPENCODE_PROJECT")
    [ "$DUMP" = "1" ]           && a+=(--dump)
    [ -n "$IP_URL" ]            && a+=(--ip-url "$IP_URL")
    a+=(--ip-interval "$IP_INTERVAL")
    eval "a+=($EXTRA_ARGS)"
    printf '%s\n' "${a[@]}"
}

bin_of() { readlink -f "/proc/$1/exe" 2>/dev/null; }

is_running() {
    [ -f "$PIDFILE" ] || return 1
    local pid; pid="$(cat "$PIDFILE")"
    { [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; } || return 1
    [ "$(bin_of "$pid")" = "$(readlink -f "$BIN")" ]
}

port_pids() {
    local out=""
    if command -v ss >/dev/null 2>&1; then
        out=$(ss -tlnp 2>/dev/null | awk -v p=":$PORT\$" '$4 ~ p')
    elif command -v netstat >/dev/null 2>&1; then
        out=$(netstat -tlnp 2>/dev/null | awk -v p=":$PORT\$" '$4 ~ p')
    fi
    echo "$out" | grep -o 'pid=[0-9]*' | cut -d= -f2 | sort -u
}

wait_dead() {
    local pid="$1" i
    for ((i=0; i<40; i++)); do
        kill -0 "$pid" 2>/dev/null || return 0
        sleep 0.2
    done
    return 1
}

# 端口被占: 是本程序的进程就 kill 等待退出, 否则报错退出脚本
resolve_conflict() {
    local pid pids
    pids="$(port_pids)"
    for pid in $pids; do
        if [ "$(bin_of "$pid")" = "$(readlink -f "$BIN")" ]; then
            echo "端口 $PORT 被本程序旧进程占用 (pid $pid), 正在停止..."
            kill "$pid" 2>/dev/null
            wait_dead "$pid" || kill -9 "$pid" 2>/dev/null
        else
            echo "错误: 端口 $PORT 被其他程序占用 (pid $pid), 退出" >&2
            exit 1
        fi
    done
}

do_stop() {
    if is_running; then
        local pid; pid="$(cat "$PIDFILE")"
        kill -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
        wait_dead "$pid" || true
        rm -f "$PIDFILE"
        echo "已停止 (pid $pid)"
    else
        rm -f "$PIDFILE"
        echo "未在运行"
    fi
}

egress_human() {
    case "$EGRESS_PREFER" in
        d4) echo "仅IPv4";;
        d6) echo "仅IPv6";;
        4)  echo "优先IPv4,回退IPv6";;
        auto) echo "双栈竞速";;
        *)  echo "优先IPv6,回退IPv4";;
    esac
}

summary() {
    echo "监听 :$PORT  后端 $BACKEND"
    echo "出口: $(egress_human)  模型替换: ${MODEL:-无}  集群: ${CLUSTER_JOIN:-${CLUSTER_LISTEN:-未启用}}"
    [ -n "$INBOUND_AUTH" ] && echo "入站鉴权: 已启用"
}

prepare() {
    mkdir -p "$LOG_DIR"
    rotate_logs
    resolve_conflict
}

cmd_start_bg() { # 后台: 日志写文件
    prepare
    local args=()
    mapfile -t args < <(build_args)
    log "worker 启动(后台): $BIN $PORT $BACKEND $(trim_args "${args[@]}")"
    nohup "$BIN" "$PORT" "$BACKEND" "${args[@]}" >> "$LOG_FILE" 2>&1 &
    local child=$!
    echo "$child" > "$PIDFILE"
    sleep 2
    if ! kill -0 "$child" 2>/dev/null; then
        echo "启动失败, 检查日志: $LOG_FILE" >&2
        rm -f "$PIDFILE"
        exit 1
    fi
    echo "已后台启动 (pid $child)"
    summary
    echo "日志: $LOG_FILE"
    echo "停止: $SELF stop"
}

cmd_run_fg() { # 前台: 日志实时输出, 同时落盘
    prepare
    local args=()
    mapfile -t args < <(build_args)
    log "worker 启动(前台): $BIN $PORT $BACKEND $(trim_args "${args[@]}")"
    echo "前台运行, Ctrl+C 停止"
    summary
    echo "------------------------------"
    trap 'kill "$CHILD" 2>/dev/null; exit 0' INT TERM
    "$BIN" "$PORT" "$BACKEND" "${args[@]}" 2>&1 | tee -a "$LOG_FILE" &
    CHILD=$!
    echo "$CHILD" > "$PIDFILE"
    wait "$CHILD"
}

cmd_status() {
    if is_running; then
        echo "运行中 (pid $(cat "$PIDFILE"), 端口 $PORT)"
    else
        local pids; pids="$(port_pids)"
        if [ -n "$pids" ]; then
            echo "未在运行 (端口 $PORT 被 pid $(echo "$pids" | tr '\n' ' ') 占用)"
        else
            echo "未在运行 (端口 $PORT)"
        fi
    fi
    [ -f "$LOG_FILE" ] && { echo "--- 最近日志 ---"; tail -n 5 "$LOG_FILE"; }
}

case "${1:-start}" in
    start)    is_running && { echo "已在运行 (pid $(cat "$PIDFILE"))"; exit 0; }; cmd_start_bg ;;
    run|-f|fg) is_running && { echo "已在运行 (pid $(cat "$PIDFILE")), 先 stop 再前台运行"; exit 0; }; cmd_run_fg ;;
    stop)     do_stop ;;
    restart)  do_stop; sleep 1; cmd_start_bg ;;
    status)   cmd_status ;;
    *)        echo "用法: $0 [start|stop|restart|status|run]"; exit 1 ;;
esac
