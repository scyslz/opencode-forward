#!/usr/bin/env bash
# opencode-zen-proxy service manager
# Usage: ./start.sh [start|stop|restart|status|run]
#   start   start in background (default), logs to file
#   run     start in foreground, logs streamed to terminal (also persisted)
#   stop    stop
#   restart restart
#   status  show status
# Interactive prompts appear only when stdin is a terminal; pipes/cron use defaults.
set -u

REPO="scyslz/opencode-forward"
TAG="${ZEN_VERSION:-v1.18.29}"
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
PROXY_PROBE_INTERVAL="${PROXY_PROBE_INTERVAL:-30s}"
IP_URL="${IP_URL:-}"
DUMP="${DUMP:-0}"
VERBOSE="${VERBOSE:-0}"
USER_AGENT="${USER_AGENT:-opencode/1.18.29 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14}"
X_OPENCODE_CLIENT="${X_OPENCODE_CLIENT:-cli}"
X_OPENCODE_PROJECT="${X_OPENCODE_PROJECT:-global}"
LOG_DIR="${LOG_DIR:-$DIR/logs}"
LOG_MAX_MB="${LOG_MAX_MB:-20}"
LOG_MAX_FILES="${LOG_MAX_FILES:-5}"

PORT="${PORT:-9000}"
INBOUND_AUTH="${INBOUND_AUTH:-}"
DNS_SERVER="${DNS_SERVER:-}"
MODEL="${MODEL:-}"
PROXY="${PROXY:-}"
CLUSTER_TOKEN="${CLUSTER_TOKEN:-}"
EXTRA_ARGS="${EXTRA_ARGS:-}"

ARCH="$(uname -m)"
case "$ARCH" in
    x86_64|amd64) ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    *) echo "Unsupported arch: $ARCH"; exit 1 ;;
esac

ensure_binary() {
    [ -x "$BIN" ] && return 0
    local url="https://github.com/${REPO}/releases/download/${TAG}/opencode-zen-proxy-linux-${ARCH}.tar.gz"
    echo "Binary not found, downloading $TAG linux-$ARCH ..."
    command -v curl >/dev/null || { echo "curl is required"; exit 1; }
    rm -rf /tmp/zen-download && mkdir -p /tmp/zen-download
    # download to file first: piping into tar breaks on slow/stalled upstreams
    curl -fsSL --connect-timeout 15 --max-time 120 -o /tmp/zen-download/pkg.tar.gz "$url" || { echo "Download failed: $url"; rm -rf /tmp/zen-download; exit 1; }
    tar -zxf /tmp/zen-download/pkg.tar.gz -C /tmp/zen-download
    mv /tmp/zen-download/opencode-zen-proxy "$BIN" && chmod +x "$BIN"
    rm -rf /tmp/zen-download
    echo "Downloaded: $BIN"
}

ask() { # $1=var name $2=prompt $3=default -> stdout
    local env="$1" p="$2" d="$3" v=""
    eval "v=\"\${$env:-}\""
    if [ ! -t 0 ]; then
        [ -n "$v" ] && { echo "$v"; return; }
        echo "$d"
        return
    fi
    local def="${v:-$d}"
    printf '%s [%s]: ' "$p" "$def" >&2
    read -r v || v=""
    echo "${v:-$def}"
}
_cmd="${1:-start}"
case "$_cmd" in
    stop|status) _interactive=0 ;;
    *) _interactive=1 ;;
esac
if [ -t 0 ] && [ "$_interactive" = 1 ]; then
    echo "== opencode-zen-proxy setup (Enter = default) =="
    PORT="$(ask PORT 'Listen port' '9003')"
    EGRESS_PREFER="$(ask EGRESS_PREFER 'Egress prefer (6/4/d4/d6/auto)' '6')"
    DNS_SERVER="$(ask DNS_SERVER 'DNS server (empty=system)' '')"
    MODEL="$(ask MODEL 'Model rewrite (empty=off)' '')"
    CLUSTER_LISTEN="$(ask CLUSTER_LISTEN 'Cluster listen addr (e.g. :62050, empty=off)' "$CLUSTER_LISTEN")"
    if [ -n "$CLUSTER_LISTEN" ]; then
        CLUSTER_JOIN=""
        echo "-> cluster listen enabled ($CLUSTER_LISTEN), skipping join (mutually exclusive)" >&2
    else
        CLUSTER_JOIN="$(ask CLUSTER_JOIN 'Cluster join URL' 'wss://cluster.oci.213470.xyz')"
    fi
    if [ -n "$CLUSTER_LISTEN" ] || [ -n "$CLUSTER_JOIN" ]; then
        CLUSTER_TOKEN="$(ask CLUSTER_TOKEN 'Cluster token' "$CLUSTER_TOKEN")"
    fi
    PROXY="$(ask PROXY 'Proxy URL (http/socks5, empty=off)' "$PROXY")"
fi
if [ -n "$CLUSTER_LISTEN" ] && [ -n "$CLUSTER_JOIN" ]; then
    echo "Warning: CLUSTER_LISTEN ($CLUSTER_LISTEN) set, ignoring CLUSTER_JOIN ($CLUSTER_JOIN) (mutually exclusive)" >&2
    CLUSTER_JOIN=""
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
    log "log rotated: $LOG_FILE.1"
}

trim_args() {
    local out="" tok
    for tok in "$@"; do
        case "$tok" in
            --cluster-token|--outbound-auth|--inbound-auth|--proxy) out+=" $tok ****" ;;
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
    [ -n "$PROXY" ]             && a+=(--proxy "$PROXY")
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
    [ -n "$PROXY" ]             && a+=(--proxy-probe-interval "$PROXY_PROBE_INTERVAL")
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

# Port conflict: kill stale instances of this binary, abort on foreign processes
resolve_conflict() {
    local pid pids
    pids="$(port_pids)"
    for pid in $pids; do
        if [ "$(bin_of "$pid")" = "$(readlink -f "$BIN")" ]; then
            echo "Port $PORT held by old instance of this binary (pid $pid), stopping it..."
            kill "$pid" 2>/dev/null
            wait_dead "$pid" || kill -9 "$pid" 2>/dev/null
        else
            echo "Error: port $PORT is used by another process (pid $pid)" >&2
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
        echo "Stopped (pid $pid)"
    else
        rm -f "$PIDFILE"
        echo "Not running"
    fi
}

egress_human() {
    case "$EGRESS_PREFER" in
        d4) echo "IPv4 only";;
        d6) echo "IPv6 only";;
        4)  echo "IPv4 preferred, fallback IPv6";;
        auto) echo "dual-stack race";;
        *)  echo "IPv6 preferred, fallback IPv4";;
    esac
}

summary() {
    echo "listen :$PORT  backend $BACKEND"
    echo "egress: $(egress_human)  proxy: ${PROXY:-off}  model rewrite: ${MODEL:-off}  cluster: ${CLUSTER_JOIN:-${CLUSTER_LISTEN:-off}}"
    [ -n "$INBOUND_AUTH" ] && echo "inbound auth: enabled"
}

prepare() {
    ensure_binary
    mkdir -p "$LOG_DIR"
    rotate_logs
    resolve_conflict
}

cmd_start_bg() {
    prepare
    local args=()
    mapfile -t args < <(build_args)
    log "worker start(bg): $BIN $PORT $BACKEND $(trim_args "${args[@]}")"
    nohup "$BIN" "$PORT" "$BACKEND" "${args[@]}" >> "$LOG_FILE" 2>&1 &
    local child=$!
    echo "$child" > "$PIDFILE"
    sleep 2
    if ! kill -0 "$child" 2>/dev/null; then
        echo "Start failed, check log: $LOG_FILE" >&2
        rm -f "$PIDFILE"
        exit 1
    fi
    echo "Started in background (pid $child)"
    summary
    echo "log: $LOG_FILE"
    echo "stop: $SELF stop"
}

cmd_run_fg() {
    prepare
    local args=()
    mapfile -t args < <(build_args)
    log "worker start(fg): $BIN $PORT $BACKEND $(trim_args "${args[@]}")"
    echo "Running in foreground, Ctrl+C to stop"
    summary
    echo "------------------------------"
    trap 'kill "$BIN_PID" 2>/dev/null; wait "$BIN_PID" 2>/dev/null; rm -f "$PIDFILE"; exit 0' INT TERM
    trap 'rm -f "$PIDFILE"' EXIT
    set +o pipefail
    set -o pipefail
    "$BIN" "$PORT" "$BACKEND" "${args[@]}" 2>&1 | tee -a "$LOG_FILE" &
    TEE_PID=$!
    # find actual binary pid (tee is the shell job pid)
    sleep 0.3
    BIN_PID="$(port_pids | head -n1)"
    if [ -z "$BIN_PID" ]; then
        BIN_PID="$TEE_PID"
    fi
    echo "$BIN_PID" > "$PIDFILE"
    wait "$TEE_PID"
}

cmd_status() {
    if is_running; then
        echo "Running (pid $(cat "$PIDFILE"), port $PORT)"
    else
        local pids; pids="$(port_pids)"
        if [ -n "$pids" ]; then
            echo "Not running (port $PORT held by pid $(echo "$pids" | tr '\n' ' '))"
        else
            echo "Not running (port $PORT)"
        fi
    fi
    [ -f "$LOG_FILE" ] && { echo "--- recent log ---"; tail -n 5 "$LOG_FILE"; }
}

case "${1:-start}" in
    start)     is_running && { echo "Already running (pid $(cat "$PIDFILE"))"; exit 0; }; cmd_start_bg ;;
    run|-f|fg) is_running && { echo "Already running (pid $(cat "$PIDFILE")), stop first for foreground"; exit 0; }; cmd_run_fg ;;
    stop)      do_stop ;;
    restart)   do_stop; sleep 1; cmd_start_bg ;;
    status)    cmd_status ;;
    *)         echo "Usage: $0 [start|stop|restart|status|run]"; exit 1 ;;
esac
