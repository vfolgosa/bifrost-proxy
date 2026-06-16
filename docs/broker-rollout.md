# Broker Rollout Behavior

## How Bifrost Handles Kafka Rolling Restarts

Kafka broker rollouts (rolling restarts) are safe with Bifrost. The proxy does **not** trigger false failovers and client applications see only transient, retriable errors — identical to connecting directly to Kafka.

## Health Check Behavior

During a broker rollout:

```
Broker A → UP  (responds to Metadata)
Broker B → DOWN (restarting)
Broker C → UP  (responds to Metadata)

Bootstrap address → routes to Broker A or C → Metadata response OK
Health check → connects to bootstrap → SASL → Metadata request → response received → PASS ✅
```

The health check connects to the **bootstrap address**, not individual brokers. As long as at least one broker in the cluster responds, the health check passes. A single broker restart does not cause the health check to fail.

**No false failover during rolling restarts.**

## Partition Leader Changes

When a broker restarts, partition leaders move to other brokers. Bifrost's leader cache may briefly have stale information:

```
T+0s    Broker B is leader of orders-0
        Leader cache: orders-0 → Broker B

T+1s    Broker B restarts
        Kafka elects Broker C as new leader of orders-0
        Leader cache: orders-0 → Broker B (stale)

T+2s    Client produces to orders-0
        Proxy routes to Broker B → connection refused (NOT_LEADER_FOR_PARTITION)
        Proxy triggers metadata refresh
        Client sees retriable error

T+3s    Client retries produce to orders-0
        Leader cache refreshed: orders-0 → Broker C
        Proxy routes to Broker C → success ✅
```

The client experiences the same transient error it would when connecting directly to Kafka. Standard Kafka client retries (`retries=3`, `retry.backoff.ms=100`) handle this transparently.

## Connection Pool

The connection pool holds warm connections to brokers. During rollout:

- Connections to the restarted broker break (`use of closed network connection`)
- Pool removes the broken connection
- Next request to that broker opens a new connection
- New connection succeeds (broker is back up)

No application impact beyond the TCP connection teardown time (~milliseconds).

## Client Impact Summary

| Scenario | Impact | Duration |
|----------|--------|----------|
| Single broker restart | Client sees NOT_LEADER → retry → success | 1-3s (client retry) |
| Multiple brokers restart (staggered) | Same as above, per affected partition | 1-3s per leader change |
| All brokers restart simultaneously | Bootstrap unreachable → health check fails → **failover triggers** | 14-30s (detection window) |
| Broker added to cluster | Metadata refresh picks up new broker, pool creates connections | Transparent |

## Avoid False Failovers

### Configure sufficient failure thresholds

Rolling restarts with 3 brokers, 30s restart each:
- Each broker is down for ~30s
- But bootstrap ALWAYS has at least 2 brokers responding
- Health check passes continuously
- `failure_threshold: 3` never triggers

```yaml
health_check:
  failure_threshold: 3     # safe for rolling restarts
  min_time_between_failovers: "60s"  # prevents flapping
```

### When false failover COULD happen

Only if ALL brokers in a cluster are restarted simultaneously (not a rolling restart). This is a full cluster outage and failover is the correct behavior.

## Load Balance During Broker Rollout

In `load_balance` mode, traffic continues to both primary and secondary clusters. A broker restart on the primary cluster causes:

- Affected partitions see transient leader-not-available errors
- Unaffected partitions continue routing normally
- Health check on primary passes (other brokers respond)
- Weights remain at 70/30
- No weight shift, no failover

**The proxy treats a single broker failure as normal Kafka operation, not as a cluster failure.**

## Recommendations

### Producer Configuration

```properties
# Ensure producers survive leader changes during rollout
retries=5
retry.backoff.ms=100
delivery.timeout.ms=120000
enable.idempotence=true
acks=all
```

### Consumer Configuration

```properties
# Consumers should not fail on transient leader changes
session.timeout.ms=30000
heartbeat.interval.ms=3000
max.poll.interval.ms=300000
```

### Proxy Configuration

```yaml
health_check:
  interval: "10s"                # normal interval
  failure_threshold: 3           # ≥3 brokers must ALL be down
  min_time_between_failovers: "60s"  # prevent flap during maintenance
  
connection_pool:
  max_connections_per_broker: 50
  idle_timeout: "30s"            # clean up connections to restarted brokers
  keep_alive_interval: "30s"     # keep connections to live brokers warm
```
