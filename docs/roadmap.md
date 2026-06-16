# Roadmap

## Next Steps

### 🔌 Custom Filters (MVP)

A filter chain that intercepts Produce/Fetch requests before routing, allowing or denying messages based on rules.

**Design:** Filters run after SASL and Metadata Rewrite, before Produce/Fetch Router. When `filters:` is empty (default), zero overhead — passthrough mode with no body parsing.

```yaml
clusters:
  logistics:
    filters:
      - type: topic_whitelist
        topics: ["orders", "shipments"]
      - type: topic_blacklist
        topics: ["debug", "test"]
      - type: message_size
        max_bytes: 1048576
      - type: header_match
        header: "env"
        value: "production"
```

**MVP scope — zero Schema Registry dependency:**

| Filter | Reads | Latency |
|--------|-------|---------|
| `topic_whitelist` / `topic_blacklist` | Topic name from Metadata/Produce header | ~1μs |
| `message_size` | Frame size from TCP layer | ~1μs |
| `header_match` | Record headers (plain key-value) | ~5μs |
| `rate_limit` | Per-topic message counter | ~2μs |

**Explicitly out of MVP scope:**
- Payload inspection (Avro/Protobuf/JSON deserialization)
- Schema Registry integration
- Record modification or rewriting
- Per-record filtering within a batch (batches are atomic)

**Future:** Schema-aware filters (v2) with local schema cache, Avro field matching.

### ⚡ Producer Audit Log

Log every Produce request with topic, partition, clientID, size, and latency. Optional sampling rate. Feeds into Prometheus or external audit system.

### 🎯 KIP-848 Native Support

Full native implementation of the new consumer group protocol (KIP-848) for KIP-848-aware brokers. Bifrost currently works with KIP-848 clients transparently (passthrough mode), but explicit awareness would enable:
- Consumer group metrics per cluster
- Rebalance-aware routing decisions
- Sticky assignment optimization

### 🌐 TLS Listener

Add optional TLS termination on proxy ports. Clients connect via `SASL_SSL` to the proxy. Proxy decrypts and forwards plain TCP upstream. Zero changes to upstream Kafka configuration.

```yaml
proxy:
  tls:
    enabled: true
    cert_file: "/etc/bifrost/cert.pem"
    key_file: "/etc/bifrost/key.pem"
```

### 🔐 Configurable Health Check SASL

Currently health check credentials come from `config.yaml`. Add support for:
- Environment variable interpolation (`${KAFKA_SASL_USER}`)
- Per-cluster SASL mechanism choice (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
- Health check SASL credentials from Kubernetes secrets

### 📊 Admin API

REST API for operational control without config file reload:
- `POST /admin/clusters/{name}/failover` — trigger manual failover
- `POST /admin/clusters/{name}/failback` — trigger manual failback
- `GET /admin/clusters/{name}/state` — detailed DR state machine status
- `PATCH /admin/clusters/{name}/weights` — override effective weights

### 📈 Per-Topic Metrics

Add Prometheus metrics with `topic` label:
- `bifrost_topic_produce_bytes_total{topic,bu}`
- `bifrost_topic_fetch_bytes_total{topic,bu}`
- `bifrost_topic_produce_rate{topic,bu}`
- `bifrost_topic_filter_denied_total{topic,rule}` (post-filters)

### ⏱️ Latency Monitoring

Track end-to-end latency per request:
- Client → proxy → upstream → proxy → client
- P50/P95/P99 histograms per cluster
- Separate metrics for Metadata, Produce, Fetch

### 🧪 Integration Test Suite

Replace shell script tests with Go integration tests:
- `TestFailover_ActivePassive`: kill primary, verify traffic shifts
- `TestFailback_ActivePassive`: restore primary, verify traffic returns
- `TestLoadBalance_StickyHash`: produce+consume with consumer group, verify partition consistency
- `TestCircuitBreaker`: trigger 3 failovers in 5min, verify manual mode

### 🔄 Async Rebalancer

Move rebalancer from health check goroutine to an independent async loop:
- Separate health detection from weight adjustment
- Health checker reports state; rebalancer reacts
- Enables per-endpoint state persistence

### 📦 Helm Chart

Production-ready Kubernetes deployment:
- Deployment with configmap-based configuration
- Service per cluster port
- Prometheus ServiceMonitor
- HPA based on connections or CPU
- PodDisruptionBudget
