#!/usr/bin/env bash
# opencode-zen-proxy 一键运行脚本 (支持 curl | bash)
# 用法: curl -fsSL <raw-url>/run.sh | bash
set -euo pipefail

REPO="scyslz/opencode-forward"
TAG="${ZEN_VERSION:-v1.18.23}"
BIN="./opencode-zen-proxy"
ARCH="$(uname -m)"
case "$ARCH" in
    x86_64|amd64) ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    *) echo "不支持的架构: $ARCH"; exit 1 ;;
esac

ask() { # $1=提示  $2=默认值  -> 结果输出到 stdout
    local p="$1" d="$2" v
    if [ -n "$d" ]; then read -r -p "$p [$d]: " v || v=""; else read -r -p "$p: " v || v=""; fi
    echo "${v:-$d}"
}

echo "=============================="
echo " opencode-zen-proxy 一键运行"
echo "=============================="

if [ ! -x "$BIN" ]; then
    echo "未找到本地二进制, 从 GitHub Release 下载 ($TAG linux-$ARCH)..."
    URL="https://github.com/${REPO}/releases/download/${TAG}/opencode-zen-proxy-linux-${ARCH}.tar.gz"
    command -v curl >/dev/null || { echo "需要 curl"; exit 1; }
    TMP="$(mktemp -d)"
    curl -fsSL "$URL" | tar -xz -C "$TMP" opencode-zen-proxy
    mv "$TMP/opencode-zen-proxy" "$BIN" && chmod +x "$BIN" && rm -rf "$TMP"
    echo "已下载: $BIN"
fi

INBOUND_AUTH="$(ask '入站鉴权 token (客户端需带 Bearer, 回车=不启用)' '')"
PORT="$(ask '本地监听端口' '9003')"
CLUSTER_LISTEN="$(ask '集群监听地址 (如 :62050, 回车=不监听)' '')"
CLUSTER_JOIN="$(ask '加入集群地址' 'wss://cluster.oci.213470.xyz')"
CLUSTER_TOKEN=""
if [ -n "$CLUSTER_JOIN" ] || [ -n "$CLUSTER_LISTEN" ]; then
    CLUSTER_TOKEN="$(ask '集群 token (回车=不加入/不启用)' '')"
fi
MODEL="$(ask '模型替换 (请求体 model 替换为该值, 回车=不替换)' '')"

ARGS=(--cache-file ./session-cache.json --egress-prefer 6)
[ -n "$INBOUND_AUTH" ]   && ARGS+=(--inbound-auth "$INBOUND_AUTH")
[ -n "$CLUSTER_LISTEN" ] && ARGS+=(--cluster-listen "$CLUSTER_LISTEN")
[ -n "$CLUSTER_TOKEN" ]  && ARGS+=(--cluster-token "$CLUSTER_TOKEN")
[ -n "$MODEL" ]          && ARGS+=(--model "$MODEL")

echo ""
echo "------------------------------"
echo " 端口: $PORT"
echo " 入站鉴权: ${INBOUND_AUTH:+已启用}${INBOUND_AUTH:-关闭}"
echo " 集群监听: ${CLUSTER_LISTEN:-无}"
if [ -n "$CLUSTER_JOIN" ] && [ -n "$CLUSTER_TOKEN" ]; then
    echo " 加入集群: $CLUSTER_JOIN"
else
    echo " 加入集群: 否"
fi
echo " 模型替换: ${MODEL:-无}"
echo "------------------------------"

if [ -n "$CLUSTER_JOIN" ] && [ -n "$CLUSTER_TOKEN" ]; then
    exec "$BIN" ":$PORT" https://opencode.ai/zen/v1 "${ARGS[@]}" \
        --cluster-join "$CLUSTER_JOIN"
else
    exec "$BIN" ":$PORT" https://opencode.ai/zen/v1 "${ARGS[@]}"
fi
