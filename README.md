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

| Mode | Behavior |
|------|----------|
| `active_passive` | DR failover. Health-based autonomous failover with state machine and circuit breaker. |
| `load_balance` | Active-active distribution with configurable weights (e.g. 70/30). Auto-rebalance on cluster failure. |
| `single` | Single cluster. No failover. Simplest setup. |

- **Autonomous Health Checks** — SASL-authenticated Metadata pings with configurable failure/recovery thresholds.
- **Live Dashboard** — per-cluster health, records produced, bytes, and failover events (Chart.js, same-origin).
- **Prometheus Metrics** — `proxy_health_status`, `proxy_connections_active`, `proxy_failover_total`.
- **Hot Reload** — edit `config.yaml` and the proxy picks up changes without restart.
- **Docker Compose** — 2 local Kafka KRaft clusters for integration testing.

## Architecture

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
- Docker & Docker Compose (for local Kafka)
- `kcat` (for testing)

### 1. Start Local Kafka Clusters

```bash
docker compose up -d
```

This starts two Kafka KRaft clusters with SASL/PLAIN on ports 19093 and 19094.

### 2. Build & Run the Proxy

```bash
go build -o bin/bifrost ./cmd/proxy/
./bin/bifrost -config config.example.yaml
```

### 3. Produce & Consume

```bash
# List topics via the proxy (logistics BU, port 9094)
kcat -b localhost:9094 \
  -X security.protocol=SASL_PLAINTEXT \
  -X sasl.mechanisms=PLAIN \
  -X sasl.username=admin \
  -X sasl.password=admin-secret \
  -L

# Produce a message
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

### 4. Open the Dashboard

```
http://localhost:8080
```

Shows health, records, bytes, and failover events per cluster.

## Configuration

See `config.example.yaml` for all options. Key sections:

```yaml
proxy:
  bind_address: "0.0.0.0"
  connection_pool:
    max_connections_per_broker: 50
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
      failure_threshold: 3
      recovery_threshold: 2
      recovery_min_uptime: "10s"
      auto_rebalance: true
      sasl_username: "admin"
      sasl_password: "admin-secret"
```

### Mode Details

See `config.example.yaml` for `active_passive`, `load_balance`, and `single` mode examples.

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
├── test/
│   ├── continuous-test.sh  # Continuous produce loop
│   └── highrate-produce.sh # High-throughput producer
├── docker-compose.yml      # 2 Kafka KRaft clusters
├── config.example.yaml     # Example configuration
├── docs/
│   └── proxy-spec.md       # Full architecture specification
└── go.mod
```

## License

MIT
