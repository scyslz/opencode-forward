# opencode-zen-proxy

opencode-zen-proxy is a lightweight reverse proxy for `opencode.ai`. It provides dual-stack egress selection and cluster failover for opencode zen backend.

## Features

- Single-port dual-stack listener (IPv6 preferred → IPv4 fallback)
- Automatic egress IP probing and failure marking (cooldown)
- Cluster mode with private TLS+frame protocol for multi-node failover
- opencode CLI feature-header injection (User-Agent, x-opencode-client/project)
- Session caching based on egress IP
- Inbound and outbound auth

## Build

```bash
go build -o opencode-zen-proxy .
```

## Configuration

| Parameter | Env | Default |
|---|---|---|
| PORT | PORT | 9000 |
| BACKEND | BACKEND | https://opencode.ai/zen/v1 |
| OUTBOUND_AUTH | OUTBOUND_AUTH | pass-through |
| INBOUND_AUTH | INBOUND_AUTH | disable |
| FWD_INBOUND | FWD_INBOUND | 0 |
| EGRESS_PREFER | EGRESS_PREFER | 6 |
| CLUSTER_TOKEN | CLUSTER_TOKEN | |
| CLUSTER_LISTEN | CLUSTER_LISTEN | |
| CLUSTER_JOIN | CLUSTER_JOIN | |
| PEERS | PEERS | |
| FAILOVER_ON | FAILOVER_ON | 429,502,503,504,timeout |
| CACHE_FILE | CACHE_FILE | logs/session-cache.json |
| IP_INTERVAL | IP_INTERVAL | 5m |

## Usage

### One-liner (curl | bash)

```bash
# 交互式: 在终端下运行会依次提问 (端口默认9003 / 入站鉴权 / 出口优先 / DNS / 集群 / 模型替换), 回车=默认
curl -fsSL https://raw.githubusercontent.com/scyslz/opencode-forward/master/start.sh -o start.sh && bash start.sh start

# 前台运行实时看日志
bash start.sh run

# 非交互式: 管道/cron 自动跳过提问, 用环境变量覆盖
PORT=9003 MODEL=gpt-5 bash start.sh start </dev/null
```

需先编译或下载二进制到同目录 (`go build -o opencode-zen-proxy .`)。

### Service management (start.sh)

| Command | Behavior |
|---|---|
| `./start.sh start` | Start in background, logs to file |
| `./start.sh run` | Start in foreground, logs streamed to terminal (also persisted) |
| `./start.sh stop` | Stop |
| `./start.sh restart` | Restart |
| `./start.sh status` | Show status + last 5 log lines |

Port conflict handling: if the port is held by an old instance of this binary it is stopped automatically and replaced; if held by another program the script exits with an error.

### Service management (start.sh)

### Cluster

```bash
# Public listener
CLUSTER_LISTEN=:9443 CLUSTER_TOKEN=s3 ./start.sh start

# Private joiner
CLUSTER_JOIN=public:9443 CLUSTER_TOKEN=s3 ./start.sh start
```

## Files

| File | Description |
|---|---|
| main.go | CLI args parsing & server |
| proxy.go | HTTP reverse proxy & failover logic |
| egress.go | dual-stack egress & probe manager |
| cluster.go | private TLS+frame cluster protocol |
| util.go | helpers |
| start.sh | unified service script (interactive/non-interactive, start/stop/restart/status/run) |
| opencode-zen-proxy.service | systemd unit |
