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
go build -o opencode-zen-proxy ./cmd/zen-proxy
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
curl -fsSL https://raw.githubusercontent.com/scyslz/opencode-forward/master/start.sh -o start.sh && bash start.sh start
```

Interactive: when run from a terminal, prompts for listen port (default 9003), inbound auth, egress preference, DNS, cluster and model rewrite — Enter accepts defaults. The binary is downloaded automatically if missing.

Foreground with live logs:

```bash
bash start.sh run
```

Non-interactive (pipes/cron skip prompts; use env vars):

```bash
PORT=9003 MODEL=gpt-5 bash start.sh start </dev/null
```

### Service management (start.sh)

| Command | Behavior |
|---|---|
| `./start.sh start` | Start in background, logs to file |
| `./start.sh run` | Start in foreground, logs streamed to terminal (also persisted) |
| `./start.sh stop` | Stop |
| `./start.sh restart` | Restart |
| `./start.sh status` | Show status + last 5 log lines |

Port conflict handling: if the port is held by an old instance of this binary it is stopped automatically and replaced; if held by another program the script exits with an error.

### Build from source

```bash
go build -o opencode-zen-proxy ./cmd/zen-proxy
OUTBOUND_AUTH="Bearer sk-..." ./start.sh start
# or via scripts/
go build -o opencode-zen-proxy ./cmd/zen-proxy && ./scripts/start.sh start
```

### Cluster

Public listener:

```bash
CLUSTER_LISTEN=:9443 CLUSTER_TOKEN=s3 ./start.sh start
```

Private joiner:

```bash
CLUSTER_JOIN=public:9443 CLUSTER_TOKEN=s3 ./start.sh start
```

## Project Layout (Standard Go)

```
cmd/zen-proxy/main.go          # entry, flag parsing, wiring
internal/proxy/                # HTTP reverse proxy & failover
internal/egress/               # dual-stack egress & probe
internal/cluster/              # private TLS+frame cluster protocol
internal/tunnel/               # tunnel persistence
internal/util/                 # helpers, constants (UpstreamTimeout)
scripts/start.sh               # service script (also ./start.sh shim)
configs/opencode-zen-proxy.service # systemd unit
legacy/                        # previous flat files (reference only)
```

| Path | Description |
|---|---|
| cmd/zen-proxy/main.go | CLI args parsing & server |
| internal/proxy | HTTP reverse proxy & failover logic |
| internal/egress | dual-stack egress & probe manager |
| internal/cluster | private TLS+frame cluster protocol |
| internal/tunnel | tunnel persistence |
| internal/util | helpers |
| scripts/start.sh | unified service script |
| configs/opencode-zen-proxy.service | systemd unit |
