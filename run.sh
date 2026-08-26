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
    if printf '%s [%s]: ' "$p" "$d" > /dev/tty 2>/dev/null; then
        read -r -t 60 v < /dev/tty || v=""
        echo "" > /dev/tty 2>/dev/null
    fi
    echo "${v:-$d}"
}

INBOUND_AUTH="$(ask INBOUND_AUTH '入站鉴权 token (回车=不启用)' '')"
PORT="$(ask PORT '本地监听端口' '9003')"
CLUSTER_LISTEN="$(ask CLUSTER_LISTEN '集群监听地址 (如 :62050, 回车=不监听)' '')"
CLUSTER_JOIN="$(ask CLUSTER_JOIN '加入集群地址' 'wss://cluster.oci.213470.xyz')"
CLUSTER_TOKEN=""
[ -n "$CLUSTER_JOIN" ] && CLUSTER_TOKEN="$(ask CLUSTER_TOKEN '集群 token (回车=不加入)' '')"
MODEL="$(ask MODEL '模型替换 (回车=不替换)' '')"

ARGS=(--cache-file "$BASE/session-cache.json" --egress-prefer 6)
[ -n "$INBOUND_AUTH" ]   && ARGS+=(--inbound-auth "$INBOUND_AUTH")
[ -n "$CLUSTER_LISTEN" ] && ARGS+=(--cluster-listen "$CLUSTER_LISTEN")
[ -n "$CLUSTER_TOKEN" ]  && ARGS+=(--cluster-token "$CLUSTER_TOKEN")
[ -n "$CLUSTER_TOKEN" ]  && ARGS+=(--cluster-join "$CLUSTER_JOIN")
[ -n "$MODEL" ]          && ARGS+=(--model "$MODEL")

echo ""
echo "------------------------------"
echo " 端口: $PORT"
[ -n "$INBOUND_AUTH" ]   && echo " 入站鉴权: 已启用"     || echo " 入站鉴权: 关闭"
[ -n "$CLUSTER_LISTEN" ] && echo " 集群监听: $CLUSTER_LISTEN"
[ -n "$CLUSTER_TOKEN" ]  && echo " 加入集群: $CLUSTER_JOIN" || echo " 加入集群: 否"
[ -n "$MODEL" ]          && echo " 模型替换: $MODEL"      || echo " 模型替换: 无"
echo " 按 Ctrl+C 停止"
echo "------------------------------"

exec "$BIN" ":$PORT" https://opencode.ai/zen/v1 "${ARGS[@]}"
