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

Interactive quick start: auto-downloads the binary, then prompts for inbound auth / local port (default 9003) / cluster listen address / cluster join URL (default wss://cluster.oci.213470.xyz) / cluster token / model rewrite. Press Enter at any prompt to accept the default.

```bash
curl -fsSL https://raw.githubusercontent.com/scyslz/opencode-forward/master/run.sh | bash
```

Download a specific version:

```bash
curl -fsSL https://raw.githubusercontent.com/scyslz/opencode-forward/master/run.sh | ZEN_VERSION=v1.18.23 bash
```

### Basic

```bash
go build -o opencode-zen-proxy .
OUTBOUND_AUTH="Bearer sk-..." ./start.sh start
```

### Cluster: Public Listener

```bash
CLUSTER_LISTEN=:9443 CLUSTER_TOKEN=s3 ./start.sh start-listen
```

### Cluster: Private Joiner

```bash
CLUSTER_JOIN=公网:9443 CLUSTER_TOKEN=s3 ./start.sh start-join
```

## Files

| File | Description |
|---|---|
| main.go | CLI args parsing & server |
| proxy.go | HTTP reverse proxy & failover logic |
| egress.go | dual-stack egress & probe manager |
| cluster.go | private TLS+frame cluster protocol |
| util.go | helpers |
| start.sh | one-shot unified service script |
| opencode-zen-proxy.service | systemd unit |
