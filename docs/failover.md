# Failover Behavior & Message Durability

## How Health Checks Detect Failures

Bifrost uses **adaptive health checks** to balance detection speed with stability:

```
Normal state:     checks every 10s (configurable)
After 1st failure: checks every 2s  (adaptive fast mode)
After recovery:   returns to 10s
```

### Detection Windows

| Failure Threshold | Normal (10s) | Adaptive (10s + 2s) |
|-------------------|-------------|---------------------|
| 1 | 10s | 10s |
| 2 | 20s | 12s |
| 3 | 30s | 14s |

The adaptive mode reduces the worst-case detection window from **30 seconds to 14 seconds** without increasing false positives — the first failure still uses the normal 10s interval, and only subsequent checks speed up.

### Failover Timeline (adaptive mode, threshold=3)

```
T+0s    Primary starts failing
T+10s   1st health check fails → adaptive fast mode (2s)
T+12s   2nd health check fails
T+14s   3rd health check fails → DECLARED DOWN
T+14s   Weights shift to 0/100, traffic goes to secondary
```

## Message Loss Risk

During the detection window (14s), the proxy still routes traffic to the failing cluster. Whether messages are lost depends on the **Kafka client configuration**, not the proxy itself.

### Risk Matrix

| Client Config | Acks | Retries | Idempotence | Loss During Failover |
|---------------|------|---------|-------------|---------------------|
| Default (librdkafka) | all | 3 | true | **None** ✅ |
| Performance tuned | 1 | 0 | false | **Possible** ⚠️ |
| Fire-and-forget | 0 | 0 | false | **Likely** 🔴 |

Bifrost does **not buffer or retry messages** — it operates at Layer 7 with stateless connection forwarding. Message durability is the responsibility of the Kafka protocol and client configuration.

## Recommended Client Configuration

### Java (Kafka Clients)

```properties
# Guaranteed delivery — zero message loss
acks=all
retries=5
retry.backoff.ms=100
enable.idempotence=true
max.in.flight.requests.per.connection=5
delivery.timeout.ms=120000
```

### librdkafka (C/C++/Python/Go)

```python
# confluent-kafka-python example
conf = {
    'acks': 'all',
    'retries': 5,
    'retry.backoff.ms': 100,
    'enable.idempotence': True,
    'max.in.flight.requests.per.connection': 5,
    'bootstrap.servers': 'proxy:9094',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': 'xxx',
    'sasl.password': 'xxx',
}
```

### kcat (Testing Only)

```bash
# kcat doesn't retry — for testing, use idempotent produce
kcat -P -b proxy:9094 \
  -X security.protocol=SASL_PLAINTEXT \
  -X enable.idempotence=true \
  -X acks=all \
  -t logistics-topic
```

## Failure Mode Analysis

### Primary Goes Down (Hard Failure)

| Phase | Duration | Traffic | Risk |
|-------|----------|---------|------|
| Detection window | ~14s | Primary still receiving | Messages in-flight may be lost if `acks < all` |
| Failover triggered | Instant | 100% to secondary | None — client retries succeed on secondary |
| Primary recovers | 10-120s | Traffic returns to primary | None — smooth weight restoration |

### Primary Goes Down (Soft Failure / High Latency)

If the primary is slow but not completely down (latency > 5s, but responds), health checks may succeed intermittently. The `consecutive_failures` counter doesn't increment, and the proxy continues routing to the degraded primary.

**Mitigation:** Reduce health check timeout or use external monitoring to detect degraded clusters.

### Both Clusters Down (BOTH_DOWN State)

If both primary and secondary fail health checks simultaneously, Bifrost enters `BOTH_DOWN` state:

- Traffic continues to the **last known healthy cluster**
- No further failover attempts (avoiding flap)
- When either cluster recovers, traffic shifts to it
- Circuit breaker may engage if this state triggers rapid transitions

## Configuration Tuning

```yaml
health_check:
  interval: "10s"              # Normal check interval
  failure_threshold: 3         # Consecutive failures to declare DOWN
                               #   With adaptive: 10+2+2 = 14s detection
                               #   Without adaptive: 10+10+10 = 30s
  recovery_threshold: 2        # Consecutive successes to declare UP
  min_time_between_failovers: "60s"  # Prevent flap between clusters
```

### Faster Detection (Higher Risk of False Positives)

```yaml
health_check:
  interval: "5s"
  failure_threshold: 2
  # Detection: 5+2 = 7s (adaptive)
  # Risk: Brief network hiccup triggers failover
```

### Conservative Detection (Lower Risk, Slower)

```yaml
health_check:
  interval: "15s"
  failure_threshold: 5
  # Detection: 15+2+2+2+2 = 23s (adaptive)
  # Risk: Longer outage before failover
```

## Summary

- **With `acks=all` + retries on the Kafka client, message loss during failover is effectively zero.**
- The adaptive health check reduces detection from 30s to ~14s with default settings.
- Bifrost is stateless — it does not buffer, queue, or retry messages. This is intentional for simplicity and performance.
- Tune `failure_threshold` and `interval` based on your tolerance for false positives vs detection speed.
