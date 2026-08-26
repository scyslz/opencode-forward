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

### Quick Start

```bash
go build -o opencode-zen-proxy .
./start.sh start
```

Interactive vs non-interactive: when run from a terminal with unset env vars, key options are prompted (port default 9003, egress preference, DNS, cluster join default wss://cluster.oci.213470.xyz, model rewrite — Enter accepts defaults); in pipes/cron all prompts are skipped and defaults/env apply.

| Command | Behavior |
|---|---|
| `./start.sh start` | Start in background, logs to file |
| `./start.sh run` | Start in foreground, logs streamed to terminal (also persisted) |
| `./start.sh stop` | Stop |
| `./start.sh restart` | Restart |
| `./start.sh status` | Show status + last 5 log lines |

Port conflict handling: if the port is held by an old instance of this binary it is stopped automatically and replaced; if held by another program the script exits with an error.

```bash
OUTBOUND_AUTH="Bearer sk-..." ./start.sh start
```

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
