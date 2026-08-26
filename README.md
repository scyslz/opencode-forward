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

Interactive quick start: auto-downloads the binary into `./opencode-zen-proxy/`, then prompts (Enter = default) for inbound auth / local port (default 9003) / egress preference / DNS server / cluster listen address / cluster join URL (default wss://cluster.oci.213470.xyz) / cluster token / model rewrite. Never hangs in non-interactive shells — prompts are skipped and defaults apply.

```bash
# interactive
curl -fsSL https://raw.githubusercontent.com/scyslz/opencode-forward/master/run.sh | bash

# background + non-interactive via env vars
curl -fsSL https://raw.githubusercontent.com/scyslz/opencode-forward/master/run.sh | bash -s -- -d

# pin a version
curl -fsSL https://raw.githubusercontent.com/scyslz/opencode-forward/master/run.sh | ZEN_VERSION=v1.18.23 bash
```

Env overrides: `PORT`, `INBOUND_AUTH`, `EGRESS_PREFER(6/4/d4/d6/auto)`, `DNS_SERVER`, `CLUSTER_LISTEN`, `CLUSTER_JOIN`, `CLUSTER_TOKEN`, `MODEL`, `DAEMON=1`.

### Service management (start.sh)

| Command | Behavior |
|---|---|
| `./start.sh start` | Start in background, logs to file |
| `./start.sh run` | Start in foreground, logs streamed to terminal (also persisted) |
| `./start.sh stop` | Stop |
| `./start.sh restart` | Restart |
| `./start.sh status` | Show status + last 5 log lines |

Interactive vs non-interactive: when run from a terminal with unset env vars, key options are prompted; in pipes/cron all prompts are skipped and defaults/env apply.

Port conflict handling: if the port is held by an old instance of this binary it is stopped automatically and replaced; if held by another program the script exits with an error.

```bash
go build -o opencode-zen-proxy .
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
| start.sh | unified service script (start/stop/restart/status/run) |
| run.sh | one-liner installer & launcher (curl \| bash) |
| opencode-zen-proxy.service | systemd unit |
