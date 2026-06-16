<p align="center">
  <img src="https://raw.githubusercontent.com/vfolgosa/bifrost-proxy/main/docs/bifrost-banner.svg" alt="Bifrost" width="600"/>
</p>

# Bifrost — Kafka L7 Proxy

> *"Route Kafka traffic across the nine realms."*

**Bifrost** is a lightweight, stateless **Layer-7 Apache Kafka proxy** written in Go. Like the rainbow bridge of Asgard, it routes Kafka protocol traffic between realms (clusters) using **port-based routing** — each Business Unit gets its own port, zero client-side changes beyond `bootstrap.servers`.

```
┌──────────┐   :9093 (finance)       ┌──────────────┐   plain TCP   ┌──────────────┐
│  Kafka   │ ──────────────────────► │              │──────────────►│  Confluent   │
│  Client  │                         │   Bifrost    │               │  Cloud (DR)  │
│          │   :9094 (logistics)     │   Proxy      │──────────────►│  Confluent   │
│          │ ──────────────────────► │              │               │  Cloud (LB)  │
└──────────┘                        └──────────────┘               └──────────────┘
     ↑                                    │                            ↑
     │  SASL credentials passthrough      │  Metadata rewrite          │
     │  (SCRAM-SHA-512 / PLAIN)           │  Health checks             │
     └────────────────────────────────────┘  Auto failover/rebalance   │
```

## Features

- **Port-Based Routing** — one port per BU. No TLS/SNI required. Client only changes `bootstrap.servers`.
- **SASL Passthrough** — forwards SCRAM-SHA-512 and PLAIN credentials transparently. Zero configuration on the proxy side.
- **Metadata Rewrite** — intercepts Kafka Metadata responses and rewrites broker addresses so clients only see the proxy.
- **Three Cluster Modes:**

| Mode | Behavior | Docs |
|------|----------|------|
| `active_passive` | DR failover. Health-based autonomous failover with state machine and circuit breaker. | [→](#) |
| `load_balance` | Active-active distribution with configurable weights (e.g. 70/30). Auto-rebalance on cluster failure. | [→](#) |
| `single` | Single cluster. No failover. Simplest setup. | [→](#) |

> 📊 **[View Interactive Modes Infographic](docs/modes.html)**

- **Autonomous Health Checks** — SASL-authenticated Metadata pings with configurable failure/recovery thresholds.
- **Live Dashboard** — per-cluster health, records produced, bytes, and failover events (Chart.js, same-origin).
- **Prometheus Metrics** — `proxy_health_status`, `proxy_connections_active`, `proxy_failover_total`.
- **Hot Reload** — edit `config.yaml` and the proxy picks up changes without restart.
- **Docker Compose** — 2 local Kafka KRaft clusters for integration testing.

## Architecture

**[→ Open full interactive architecture diagram](docs/architecture.html)**

```
                    ┌────────────────────────────┐
                    │          Bifrost            │
                    │                            │
  Client ──:9093──► │  TCP Listener (multi-port)  │
  Client ──:9094──► │                            │
                    │  ┌──────────────────────┐  │
                    │  │   SASL Passthrough    │  │
                    │  │   Metadata Rewrite    │  │
                    │  │   Produce/Fetch Route │  │
                    │  └──────────────────────┘  │
                    │                            │
                    │  ┌──────────────────────┐  │
                    │  │   Health Checker      │──► Kafka Metadata ping
                    │  │   Rebalancer          │──► Weight adjustment
                    │  │   Circuit Breaker     │──► Flap protection
                    │  └──────────────────────┘  │
                    │                            │
                    │  :8080 ── Dashboard + Metrics
                    └────────────────────────────┘
```

## Quick Start

### Prerequisites

- Go 1.22+
- Docker & Docker Compose
- `kcat` (for testing)

### 1. Start Everything

```bash
docker compose up -d
```

Starts 2 Kafka KRaft clusters + Bifrost proxy + Redpanda consoles + Prometheus.

### 2. Produce & Consume

```bash
# List topics via the proxy (logistics BU, port 9094)
kcat -b localhost:9094 \
  -X security.protocol=SASL_PLAINTEXT \
  -X sasl.mechanisms=PLAIN \
  -X sasl.username=admin -X sasl.password=admin-secret \
  -L

# Produce
echo "hello bifrost" | kcat -P -b localhost:9094 \
  -X security.protocol=SASL_PLAINTEXT \
  -X sasl.mechanisms=PLAIN \
  -X sasl.username=admin -X sasl.password=admin-secret \
  -t logistics-topic

# Consume
kcat -C -b localhost:9094 \
  -X security.protocol=SASL_PLAINTEXT \
  -X sasl.mechanisms=PLAIN \
  -X sasl.username=admin -X sasl.password=admin-secret \
  -t logistics-topic -o beginning -e
```

### 3. Open the Dashboard

```
http://localhost:8080
```

## Monitoring Stack

| Service | URL |
|---------|-----|
| Bifrost Dashboard | http://localhost:8080 |
| Prometheus | http://localhost:9090 |
| Redpanda (kafka1) | http://localhost:8081 |
| Redpanda (kafka2) | http://localhost:8082 |

## Configuration

See `config.example.yaml` for all options with comments.

```yaml
proxy:
  bind_address: "0.0.0.0"
  metrics_port: 8080

clusters:
  logistics:
    port: 9094
    mode: "load_balance"
    primary:
      bootstrap: "pkc-xxxx.us-east-1.aws.confluent.cloud:9092"
      weight: 70
    secondary:
      bootstrap: "pkc-yyyy.us-east-2.aws.confluent.cloud:9092"
      weight: 30
    health_check:
      enabled: true
      interval: "10s"
      auto_rebalance: true
      sasl_username: "admin"
      sasl_password: "admin-secret"
```

## Docs & Assets

| Asset | Description |
|-------|-------------|
| [Logo](docs/logo.html) | Bifrost brand mark — rainbow bridge |
| [Architecture](docs/architecture.html) | Full system diagram (SVG, dark theme) |
| [Modes](docs/modes.html) | Visual comparison of all 3 cluster modes |
| [Social Preview](docs/social-preview.html) | 1280×640 GitHub OpenGraph card |
| [Spec](docs/proxy-spec.md) | Full technical specification |

## Project Structure

```
bifrost-proxy/
├── cmd/proxy/              # Entry point
├── internal/
│   ├── config/             # YAML parsing, validation, hot reload
│   ├── protocol/           # Kafka wire protocol parser
│   ├── proxy/              # TCP listener, connection handler, routing
│   ├── routing/            # SASL, metadata, produce/fetch routing
│   ├── pool/               # Connection pool, leader cache
│   ├── health/             # Health check engine
│   ├── failover/           # State machine, controller, rebalance
│   ├── logger/             # Structured JSON logging
│   └── server/             # HTTP observability server + dashboard
├── test/                   # Test scripts
├── docs/                   # Documentation & assets
├── docker-compose.yml      # Full local dev stack
├── config.example.yaml     # Example configuration
└── go.mod
```

## License

MIT
