<p align="center">
  <img src="docs/bifrost-banner.svg" alt="Bifrost" width="600"/>
</p>

# Bifrost — Kafka L7 Proxy

> *"Route Kafka traffic across the nine realms."*

**Bifrost** is a lightweight, stateless **Layer-7 Apache Kafka proxy** written in Go. Like the rainbow bridge of Asgard, it routes Kafka protocol traffic between realms (clusters) using **port-based routing** — each Business Unit gets its own port, zero client-side changes beyond `bootstrap.servers`.

<p align="center">
  <a href="docs/architecture.html">🏗️ Architecture Diagram</a> ·
  <a href="docs/modes.html">📊 Cluster Modes</a> ·
  <a href="docs/logo.html">🌈 Logo</a> ·
  <a href="SECURITY.md">🔒 Security</a>
</p>

## ✨ Features

| | |
|---|---|
| 🔌 **Port-Based Routing** | One port per BU. No TLS/SNI required. Client only changes `bootstrap.servers`. |
| 🔐 **SASL Passthrough** | Forwards SCRAM-SHA-512 and PLAIN credentials transparently. Zero proxy-side config. |
| 📝 **Metadata Rewrite** | Intercepts Kafka Metadata responses, rewrites broker addresses so clients see only the proxy. |
| 🎛️ **Three Modes** | `active_passive` (DR failover) · `load_balance` (weighted distribution) · `single` (standalone) |
| ❤️ **Health Checks** | SASL-authenticated Metadata pings with configurable failure/recovery thresholds. |
| 📈 **Live Dashboard** | Per-cluster health, records, bytes, and failover events (Chart.js, embedded). |
| 🔥 **Hot Reload** | Edit `config.yaml` — proxy picks up changes without restart. |
| 📡 **Prometheus** | `proxy_health_status`, `proxy_connections_active`, `proxy_failover_total`. |

> 🏗️ **[View full architecture diagram →](docs/architecture.html)** · 📊 **[Compare cluster modes →](docs/modes.html)**

## 🚀 Quick Start

```bash
# 1. Start everything (Kafka + Bifrost + Redpanda + Prometheus)
docker compose up -d

# 2. Produce a message
echo "hello bifrost" | kcat -P -b localhost:9094 \
  -X security.protocol=SASL_PLAINTEXT \
  -X sasl.mechanisms=PLAIN \
  -X sasl.username=admin -X sasl.password=admin-secret \
  -t logistics-topic

# 3. Consume
kcat -C -b localhost:9094 \
  -X security.protocol=SASL_PLAINTEXT \
  -X sasl.mechanisms=PLAIN \
  -X sasl.username=admin -X sasl.password=admin-secret \
  -t logistics-topic -o beginning -e

# 4. Open the dashboard
open http://localhost:8080
```

## 📊 Monitoring

| Service | URL |
|---------|-----|
| Bifrost Dashboard | http://localhost:8080 |
| Prometheus | http://localhost:9090 |
| Redpanda kafka1 | http://localhost:8081 |
| Redpanda kafka2 | http://localhost:8082 |

## ⚙️ Configuration

```yaml
proxy:
  bind_address: "0.0.0.0"
  metrics_port: 8080

clusters:
  # active_passive — DR failover
  finance:
    port: 9093
    mode: "active_passive"
    active: "primary"
    primary: "pkc-11111.us-east-1.aws.confluent.cloud:9092"
    secondary: "pkc-22222.us-east-2.aws.confluent.cloud:9092"
    health_check:
      enabled: true
      auto_failover: true
      auto_failback: false

  # load_balance — weighted distribution
  logistics:
    port: 9094
    mode: "load_balance"
    primary:
      bootstrap: "pkc-33333.us-east-1.aws.confluent.cloud:9092"
      weight: 70
    secondary:
      bootstrap: "pkc-44444.us-east-2.aws.confluent.cloud:9092"
      weight: 30
    health_check:
      enabled: true
      auto_rebalance: true

  # single — standalone cluster
  # legacy:
  #   port: 9095
  #   mode: "single"
  #   primary: "old-kafka.internal:9092"
```

> 📋 **[Full config example with all options →](config.example.yaml)**

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
├── test/                   # Test scripts
├── docs/                   # Docs & visual assets
│   ├── architecture.html   # Interactive architecture diagram
│   ├── modes.html          # Cluster modes comparison
│   ├── logo.html           # Brand logo
│   ├── social-preview.html # GitHub OpenGraph card
│   └── proxy-spec.md       # Full technical spec
├── docker-compose.yml      # Full dev stack
├── Dockerfile              # Multi-stage build
├── config.example.yaml     # Example config
└── go.mod
```

## 📄 License

MIT
