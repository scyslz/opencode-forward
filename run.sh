#!/usr/bin/env bash
# opencode-zen-proxy 一键运行脚本 (支持 curl | bash)
# 用法: curl -fsSL <raw-url>/run.sh | bash
# 非交互环境(管道/webhook): 不提问, 全部用默认值或同名环境变量覆盖
set -u

REPO="scyslz/opencode-forward"
TAG="${ZEN_VERSION:-v1.18.23}"

# 所有文件统一放一个目录: 当前目录下的 opencode-zen-proxy/
BASE="$PWD/opencode-zen-proxy"
mkdir -p "$BASE"
BIN="$BASE/opencode-zen-proxy"

ARCH="$(uname -m)"
case "$ARCH" in
    x86_64|amd64) ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    *) echo "不支持的架构: $ARCH"; exit 1 ;;
esac

echo "=============================="
echo " opencode-zen-proxy 一键运行"
echo " 目录: $BASE"
echo "=============================="

if [ ! -x "$BIN" ]; then
    URL="https://github.com/${REPO}/releases/download/${TAG}/opencode-zen-proxy-linux-${ARCH}.tar.gz"
    echo "下载 $TAG linux-$ARCH ..."
    rm -rf /tmp/zen-download && mkdir -p /tmp/zen-download
    curl -fsSL "$URL" | tar -zxvf - -C /tmp/zen-download
    mv /tmp/zen-download/opencode-zen-proxy "$BIN" && chmod +x "$BIN"
    rm -rf /tmp/zen-download
    echo "已下载: $BIN"
fi

# 有控制终端(/dev/tty)就提问, 60秒无输入自动用默认值; 无终端直接默认
ask() { # $1=环境变量名 $2=提示 $3=默认值 -> stdout
    local env="$1" p="$2" d="$3" v=""
    eval "v=\"\${$env:-}\""
    [ -n "$v" ] && { echo "$v"; return; }
    if { printf '%s [%s]: ' "$p" "$d" 2>/dev/null > /dev/tty; } 2>/dev/null; then
        read -r -t 60 v < /dev/tty || v=""
        echo "" > /dev/tty 2>/dev/null
    fi
    echo "${v:-$d}"
}

INBOUND_AUTH="$(ask INBOUND_AUTH '入站鉴权 token (回车=不启用)' '')"
PORT="$(ask PORT '本地监听端口' '9003')"
EGRESS_PREFER="$(ask EGRESS_PREFER '出口优先 (6=优先IPv6回退IPv4 / 4=优先IPv4回退IPv6 / d4=仅IPv4 / d6=仅IPv6 / auto)' '6')"
DNS_SERVER="$(ask DNS_SERVER 'DNS 服务器 (如 8.8.8.8, 回车=系统默认)' '')"
CLUSTER_LISTEN="$(ask CLUSTER_LISTEN '集群监听地址 (如 :62050, 回车=不监听)' '')"
CLUSTER_JOIN="$(ask CLUSTER_JOIN '加入集群地址' 'wss://cluster.oci.213470.xyz')"
CLUSTER_TOKEN=""
[ -n "$CLUSTER_JOIN" ] && CLUSTER_TOKEN="$(ask CLUSTER_TOKEN '集群 token (回车=不加入)' '')"
MODEL="$(ask MODEL '模型替换 (回车=不替换)' '')"

ARGS=(--cache-file "$BASE/session-cache.json" --egress-prefer "$EGRESS_PREFER")
[ -n "$INBOUND_AUTH" ]   && ARGS+=(--inbound-auth "$INBOUND_AUTH")
[ -n "$DNS_SERVER" ]     && ARGS+=(--dns-server "$DNS_SERVER")
[ -n "$CLUSTER_LISTEN" ] && ARGS+=(--cluster-listen "$CLUSTER_LISTEN")
[ -n "$CLUSTER_TOKEN" ]  && ARGS+=(--cluster-token "$CLUSTER_TOKEN")
[ -n "$CLUSTER_TOKEN" ]  && ARGS+=(--cluster-join "$CLUSTER_JOIN")
[ -n "$MODEL" ]          && ARGS+=(--model "$MODEL")

# 启动前检查端口占用
port_busy() {
    if command -v ss >/dev/null 2>&1; then
        ss -tln 2>/dev/null | awk '{print $4}' | grep -qE "[:.]${PORT}$" && return 0
    elif command -v netstat >/dev/null 2>&1; then
        netstat -tln 2>/dev/null | awk '{print $4}' | grep -qE "[:.]${PORT}$" && return 0
    else
        (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && return 0
    fi
    return 1
}

if port_busy; then
    echo "错误: 端口 $PORT 已被占用, 换一个或先停掉占用进程"
    exit 1
fi

# 后台启动: 第一个参数 -d / --daemon, 或环境变量 DAEMON=1
DAEMON="${DAEMON:-}"
[ "$1" = "-d" ] || [ "$1" = "--daemon" ] && DAEMON=1

echo ""
echo "------------------------------"
echo " 端口: $PORT"
case "$EGRESS_PREFER" in
    d4) echo " 出口: 仅IPv4" ;;
    d6) echo " 出口: 仅IPv6" ;;
    4)  echo " 出口: 优先IPv4,回退IPv6" ;;
    auto) echo " 出口: 双栈竞速" ;;
    *) echo " 出口: 优先IPv6,回退IPv4" ;;
esac
[ -n "$DNS_SERVER" ]     && echo " DNS: $DNS_SERVER"
[ -n "$INBOUND_AUTH" ]   && echo " 入站鉴权: 已启用"     || echo " 入站鉴权: 关闭"
[ -n "$CLUSTER_LISTEN" ] && echo " 集群监听: $CLUSTER_LISTEN"
[ -n "$CLUSTER_TOKEN" ]  && echo " 加入集群: $CLUSTER_JOIN" || echo " 加入集群: 否"
[ -n "$MODEL" ]          && echo " 模型替换: $MODEL"      || echo " 模型替换: 无"

if [ "$DAEMON" = "1" ]; then
    nohup "$BIN" ":$PORT" https://opencode.ai/zen/v1 "${ARGS[@]}" \
        > "$BASE/opencode-zen-proxy.log" 2>&1 &
    echo $! > "$BASE/opencode-zen-proxy.pid"
    sleep 1
    if kill -0 "$(cat "$BASE/opencode-zen-proxy.pid")" 2>/dev/null; then
        echo " 已后台启动 (pid $(cat "$BASE/opencode-zen-proxy.pid"))"
        echo " 日志: $BASE/opencode-zen-proxy.log"
        echo " 停止: kill \$(cat $BASE/opencode-zen-proxy.pid)"
    else
        echo " 启动失败, 查看日志: $BASE/opencode-zen-proxy.log"
        exit 1
    fi
else
    echo " 按 Ctrl+C 停止"
    echo "------------------------------"
    exec "$BIN" ":$PORT" https://opencode.ai/zen/v1 "${ARGS[@]}"
fi
