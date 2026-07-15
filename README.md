<p align="center">
  <img src="assets/bifrost-banner.svg" alt="Bifrost" width="600"/>
</p>

# Bifrost — Kafka L7 Proxy

> *"Route Kafka traffic across the nine realms."*

**Bifrost** is a lightweight, stateless **Layer-7 Apache Kafka proxy** written in Go. Routes Kafka protocol traffic using **port-based routing** — each BU gets its own port, zero client-side changes beyond `bootstrap.servers`.

## ✨ Features

| | |
|---|---|
| 🔌 **Port-Based Routing** | One port per BU. No TLS/SNI required. |
| 🔐 **SASL Passthrough** | Forwards SCRAM-SHA-512 and PLAIN credentials transparently. |
| 📝 **Metadata Rewrite** | Intercepts Metadata responses, rewrites broker addresses to `advertise_host`. |
| 🎛️ **Three Modes** | `active_passive` · `load_balance` · `single` |
| ❤️ **Health Checks** | SASL-authenticated Metadata pings with configurable thresholds. |
| 🔄 **Autonomous Failover** | `auto_failover` (active_passive) and `auto_rebalance` (load_balance). |
| 📈 **Live Dashboard** | Per-cluster health, effective weights, drain and failover state. |
| 🔥 **Hot Reload** | Edit `config.yaml` — picks up changes without restart. |
| 📡 **Prometheus** | `proxy_health_status` · `proxy_failover_total` · `proxy_connections_active` |

## 🏗️ Architecture

<p align="center">
  <img src="assets/architecture.svg" alt="Bifrost Architecture" width="100%"/>
</p>

## 🎛️ Cluster Modes

<p align="center">
  <img src="assets/modes.svg" alt="Cluster Modes" width="100%"/>
</p>

## 📋 Prerequisites

| Tool | Purpose |
|------|---------|
| [Docker](https://docs.docker.com/get-docker/) + Docker Compose | Local Kafka stack |
| [Go](https://go.dev/dl/) 1.23+ | Build and run the proxy on the host |
| [kcat](https://github.com/edenhill/kcat) | Produce/consume smoke tests |
| `curl`, `python3` | HTTP checks and `/status` parsing |

## 🔌 Ports (local dev)

| Port | Service |
|------|---------|
| **9093** | Bifrost — cluster `finance` (`active_passive`) |
| **9094** | Bifrost — cluster `logistics` (`load_balance`) |
| **8080** | Bifrost HTTP — dashboard, `/health`, `/status`, `/metrics` |
| **19093** | Kafka upstream — `kafka1` (SASL/PLAIN, **not** a proxy port) |
| **19094** | Kafka upstream — `kafka2` (SASL/PLAIN, **not** a proxy port) |
| **8081** | Redpanda Console — kafka1 |
| **8082** | Redpanda Console — kafka2 |
| **9090** | Prometheus |

Clients connect to **9093/9094** (proxy). Health checks and leader-cache refresh use the upstream bootstrap addresses configured in YAML (e.g. `localhost:19093`).

## 🚀 Local Development

Two ways to run locally. **Option B (proxy on host)** is recommended for full testing of both clusters (`finance` + `logistics`).

### Step 0 — Kafka JAAS secrets

`docker-compose.yml` mounts SASL config files that are gitignored. Create them once:

```bash
mkdir -p secrets
cat > secrets/kafka1_jaas.conf <<'EOF'
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="admin"
  password="admin-secret"
  user_admin="admin-secret";
};
EOF
cp secrets/kafka1_jaas.conf secrets/kafka2_jaas.conf
```

### Step 1 — Start Kafka

```bash
docker compose up -d kafka1 kafka2 kafka-init
```

`kafka-init` creates `logistics-topic` on both brokers. Create `finance-topic` manually:

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
  --create --topic finance-topic --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka2 kafka-topics --bootstrap-server localhost:9092 \
  --create --topic finance-topic --partitions 3 --replication-factor 1 --if-not-exists
```

### Option A — Proxy in Docker (logistics only)

The `bifrost` service in `docker-compose.yml` exposes ports **8080** and **9094** only (not 9093).

```bash
# Config for in-container networking (PLAINTEXT to kafka1/kafka2)
cp test/fixtures/regression-config.yaml config.local.yaml
# Edit bootstraps: primary/secondary → kafka1:9092 / kafka2:9092

docker compose up -d bifrost redpanda-kafka1 redpanda-kafka2 prometheus
```

### Option B — Proxy on host (recommended)

Use the regression fixture as a starting point — bootstraps point at `localhost:19093/19094`:

```bash
cp test/fixtures/regression-config.yaml config.yaml

go build -o bifrost ./cmd/proxy
./bifrost -config config.yaml
```

Optional: start observability sidecars without the in-compose proxy:

```bash
docker compose up -d redpanda-kafka1 redpanda-kafka2 prometheus
```

### Smoke test

```bash
# SASL flags reused below
export SASL='-X security.protocol=SASL_PLAINTEXT -X sasl.mechanisms=PLAIN -X sasl.username=admin -X sasl.password=admin-secret'

# finance (active_passive) — port 9093
echo "hello finance" | kcat -P -b localhost:9093 $SASL -t finance-topic
kcat -C -b localhost:9093 $SASL -t finance-topic -o -1 -e

# logistics (load_balance) — port 9094
echo "hello logistics" | kcat -P -b localhost:9094 $SASL -t logistics-topic
kcat -C -b localhost:9094 $SASL -t logistics-topic -o -1 -e

# Dashboard
open http://localhost:8080
```

### Automated tests

```bash
# Quick smoke (proxy must already be running)
bash test/test-all.sh

# Full regression: brings up Kafka, builds proxy, runs unit + e2e tests, writes report
bash test/full-local-regression.sh

# Keep environment running after regression
KEEP_ENV=1 bash test/full-local-regression.sh
```

Reports are written to `test/reports/regression-*.md`.

## 📊 Monitoring

| Service | URL |
|---------|-----|
| Bifrost Dashboard | http://localhost:8080 |
| Bifrost `/status` | http://localhost:8080/status |
| Bifrost `/metrics` | http://localhost:8080/metrics |
| Prometheus | http://localhost:9090 |
| Redpanda kafka1 | http://localhost:8081 |
| Redpanda kafka2 | http://localhost:8082 |

### HTTP endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Liveness probe (`{"status":"ok"}`) |
| `GET /status` | JSON snapshot — per-cluster health, **effective weights**, **effective active** (includes autonomous failover), drain state |
| `GET /metrics` | Prometheus text format |
| `GET /topic-stats` | Produce/fetch counters per topic and cluster |
| `GET /` | Embedded dashboard |

## 🔄 Failover mechanisms

Three independent mechanisms coexist:

| Mechanism | Mode | Trigger | Config flag |
|-----------|------|---------|-------------|
| **Hot reload** | `active_passive` | Edit `active: "primary"` / `"secondary"` in YAML | — |
| **Autonomous failover** | `active_passive` | Health check detects primary down | `auto_failover: true` |
| **Auto-rebalance** | `load_balance` | Health check shifts weights (e.g. 70/30 → 0/100) | `auto_rebalance: true` |

- Hot reload transitions through **DRAINING** (graceful connection drain) before switching.
- Autonomous failover uses the same drain workflow via the DR state machine.
- `/status` shows the **effective** `active` cluster and **effective** weights (post-rebalance), not only the YAML values.

See [Failover & Message Durability](docs/failover.md) for detection windows and client implications.

## ⚙️ Configuration

Copy `config.example.yaml` to `config.yaml` (or `config.local.yaml` for Docker). Full reference:

```yaml
proxy:
  bind_address: "0.0.0.0"          # TCP listen address for Kafka ports
  advertise_host: "localhost"       # hostname clients see in rewritten Metadata
  metrics_port: 8080
  connection_pool:
    max_connections_per_broker: 50
    idle_timeout: "30s"             # drain timeout for graceful failover
    keep_alive_interval: "30s"

clusters:
  finance:
    port: 9093                       # dedicated client port for this BU
    mode: "active_passive"
    active: "primary"                # "primary" | "secondary" — hot-reload target
    primary: "host:9092"             # upstream bootstrap (string in active_passive)
    secondary: "host:9092"
    health_check:
      enabled: true
      interval: "10s"
      failure_threshold: 3           # consecutive failures → DOWN
      recovery_threshold: 2          # consecutive successes → UP
      min_time_between_failovers: "60s"
      auto_failover: true            # autonomous DR on health events
      auto_failback: false           # return to primary when it recovers
      require_target_healthy: true
      circuit_breaker_max_failovers: 3
      circuit_breaker_window: "300s"
      sasl_username: ""              # required when upstream uses SASL
      sasl_password: ""

  logistics:
    port: 9094
    mode: "load_balance"
    primary:
      bootstrap: "host:9092"
      weight: 70                     # must sum to 100 with secondary
    secondary:
      bootstrap: "host:9092"
      weight: 30
    health_check:
      enabled: true
      interval: "10s"
      failure_threshold: 3
      recovery_threshold: 2
      recovery_min_uptime: "120s"    # delay before restoring primary weight
      auto_rebalance: true           # shift weights on health transitions
      sasl_username: "admin"
      sasl_password: "admin-secret"

  # single — one cluster, no failover
  # legacy:
  #   port: 9095
  #   mode: "single"
  #   primary: "old-kafka.internal:9092"
```

### Key fields

| Field | Notes |
|-------|-------|
| `advertise_host` | Hostname/IP written into Metadata responses so clients reconnect to the proxy, not upstream brokers. Use the hostname clients actually reach (e.g. load balancer DNS in prod, `localhost` in dev). |
| `health_check.sasl_*` | Credentials for health checks **and** leader-cache metadata refresh against SASL brokers. |
| `auto_failover` | Only for `active_passive`. When `false`, failover is manual via hot reload only. |
| `auto_rebalance` | Only for `load_balance`. When `false`, weights stay at configured values regardless of health. |

### Hot reload

Edit `config.yaml` on disk — the proxy watches the file and applies changes without restart. Changing `active:` triggers a drain cycle before routing switches.

### CLI

```bash
bifrost -config /path/to/config.yaml
```

Default config path: `config.yaml` in the working directory.

## 📚 Documentation

| Doc | Description |
|-----|-------------|
| [Consumer Behavior](docs/consumer-behavior.md) | How consumers work with load balancing, failover, ordering, and deduplication |
| [Failover & Message Durability](docs/failover.md) | Adaptive health checks, detection windows, message loss risk, client config |
| [Broker Rollout](docs/broker-rollout.md) | Proxy behavior during Kafka rolling restarts and leader changes |
| [Roadmap](docs/roadmap.md) | Planned features and next steps |
| [SECURITY.md](SECURITY.md) | Production hardening, secrets, known limitations |

## 📁 Project Structure

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
│   └── server/             # HTTP server + embedded dashboard
├── test/
│   ├── test-all.sh         # Quick smoke tests
│   ├── full-local-regression.sh
│   └── fixtures/           # Local dev / regression config
├── assets/                 # Diagrams and branding
├── docker-compose.yml      # Kafka + optional proxy stack
├── Dockerfile              # Multi-stage build
└── config.example.yaml     # Production-oriented example config
```

## 📄 License

MIT
