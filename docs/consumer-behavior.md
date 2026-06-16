# Consumer Behavior with Load Balancing

## How Routing Works for Consumers

Bifrost routes **all** Kafka protocol requests — including Fetch (consume) — through the same routing logic:

| Request Type | API Key | Routed? | How |
|-------------|---------|---------|-----|
| Metadata | 3 | Yes | Synthetic merge from both clusters |
| Produce | 0 | Yes | Sticky hash: `(topic, partition)` |
| **Fetch** | **1** | **Yes** | **Sticky hash: `(topic, partition)`** |
| Offsets | 2 | Yes | Sticky hash |
| JoinGroup | 11 | Yes | Passthrough to current upstream |

### Partition-Level Sticky Routing

```
StickyHash(topic, partition) % 100

  hash < 70  →  Primary (70% traffic)
  hash >= 70 →  Secondary (30% traffic)
```

The hash depends only on **(topic, partition)** — NOT on clientID. This means **every client** (producers and consumers) routes to the same cluster for a given partition. This is essential when clusters are **not mirrored**: each partition has a fixed "owner" cluster, and data is always where consumers expect it.

## What This Means for Consumers

### A Consumer Group Consuming Topic "orders"

```
orders-0  →  hash=45  →  Primary   ← Consumer A fetches from Primary
orders-1  →  hash=82  →  Secondary ← Consumer A fetches from Secondary
orders-2  →  hash=23  →  Primary   ← Consumer B fetches from Primary
orders-3  →  hash=15  →  Primary   ← Consumer B fetches from Primary
orders-4  →  hash=91  →  Secondary ← Consumer C fetches from Secondary
```

**One consumer may fetch from both clusters.** The consumer doesn't know — it only sees the proxy. Metadata is synthetically merged, so the client sees a unified broker list with all partitions.

### Topic Requirements

Both clusters **must have identical topic and partition layouts**. The synthetic metadata response merges both clusters. If a partition exists on one cluster but not the other, consumers will get errors when fetching from the missing cluster.

```
✅ Primary:  orders (5 partitions) + Secondary:  orders (5 partitions)
❌ Primary:  orders (5 partitions) + Secondary:  orders (3 partitions)
❌ Primary:  orders (5 partitions) + Secondary:  NO orders topic
```

**Create topics manually on both clusters** before enabling routing. Bifrost does not replicate topics or data between clusters.

## Consumer Group Coordination

Consumer group operations (JoinGroup, SyncGroup, Heartbeat) are forwarded to whichever cluster the connection is currently routed to. During normal operation this is the primary. During failover, new connections route to secondary — and coordination moves there too. This ensures:

- Consumer group state survives failover
- Offset commits continue working
- Rebalance decisions happen on the active cluster

When the primary recovers, new connections route back to primary, and coordination gradually returns.

## Client Configuration

### Zero Changes Required

The consumer connects to the proxy exactly as it would connect to any Kafka cluster:

```properties
# Java consumer — nothing special needed
bootstrap.servers=proxy:9094
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="xxx" password="xxx";
group.id=my-consumer-group
auto.offset.reset=earliest
```

```python
# confluent-kafka-python — identical to direct Kafka connection
consumer = Consumer({
    'bootstrap.servers': 'proxy:9094',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': 'xxx',
    'sasl.password': 'xxx',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest',
})
```

The proxy handles all the complexity transparently.

## Edge Cases

### Failover While Consuming

```
T+0s    Consumer fetching orders-0 from Primary
T+1s    Primary fails health checks
T+15s   Primary declared DOWN, weight shifts to 0/100
T+15s   Existing connection broken → Kafka client reconnects
T+16s   New connection → selectLoadBalanceTarget → secondary (primary weight=0)
T+16s   JoinGroup, Heartbeat, OffsetCommit, Fetch — all to Secondary
```

The consumer may see brief errors (connection closed, timeout) during the detection window. Standard Kafka client retries handle this transparently. After reconnection, all operations continue on Secondary.

### Data Locality During Failover

Since clusters are NOT mirrored, data for a partition lives on exactly one cluster:

```
Normal:   p0 → Primary [msg-1, msg-2, msg-3]
Failover: p0 → Secondary ← EMPTY for p0 (no mirroring)
```

Messages produced during failover for p0 go to Secondary. After failback, consumers reading p0 from Primary won't see those messages — they're isolated on Secondary.

This is a **data locality trade-off**, not a bug. See [Message Ordering & Deduplication](#message-ordering--deduplication) below for mitigation strategies.

### Scaling Consumers

Consumer scaling is transparent. The hash depends on `(topic, partition)` — not on which consumer is assigned. Whether 1 consumer or 100, partition 0 always routes to Primary:

```
3 consumers → scale to 4:
  Before:  A: [p0, p1]  B: [p2, p3]  C: [p4, p5]
  After:   A: [p0, p3]  B: [p1, p4]  C: [p2]  D: [p5]
  
  p0 always → Primary   (no matter who consumes it)
  p1 always → Secondary
```

Rebalance just changes **who** consumes each partition. **Where** is fixed by the hash.

## Summary

| Question | Answer |
|----------|--------|
| Are consumers load-balanced? | **Yes**, at the partition level via sticky hash |
| Does the consumer need to change? | **No** — it connects to one proxy address |
| Can one consumer fetch from both clusters? | **Yes** — if its partitions hash to both sides |
| Does scaling change routing? | **No** — hash is by partition, not consumer |
| Is consumer coordination available during failover? | **Yes** — moves to Secondary on new connections |
| Must topics be mirrored? | **No** — but identical partition layouts required |
| What happens on failover? | Kafka client retry → transparent failover to Secondary |

## Message Ordering & Deduplication

### Partition-Level Guarantees

Within a single partition, Bifrost preserves Kafka's ordering guarantees during normal operation:

```
Partition 0 → Primary   (ALL messages for p0 go here)
  msg-1, msg-2, msg-3, msg-4  ✅ ordered
```

The sticky hash `(topic, partition)` is deterministic — every message for partition 0 always routes to the same cluster. Ordering within that partition on that cluster is guaranteed by Kafka itself.

### Failover Impact on Ordering

When the primary cluster fails and traffic shifts to secondary, the partition "owner" changes:

```
Normal (70/30):
  p0 → Primary     [msg-1, msg-2, msg-3]
  p1 → Secondary   [msg-4, msg-5]

Failover (0/100):
  p0 → Secondary   ← NOW routes here (no historical data for p0)
  p1 → Secondary   [msg-4, msg-5]

Failback (70/30):
  p0 → Primary     ← returns here
```

**Problem:** Messages produced during failover for partition 0 live on Secondary, but after failback, consumers read from Primary — where those messages don't exist. Offsets committed during failover also live on Secondary. On failback, the consumer fetches offsets from Primary and may re-read messages already consumed from Secondary.

### Impact Summary

| Scenario | Behavior | Impact |
|----------|----------|--------|
| Normal operation | All p0 → Primary | ✅ Guaranteed ordering |
| During failover | p0 → Secondary | ⚠️ Data isolated on Secondary |
| After failback | p0 → Primary | 🔴 Duplicates possible |
| Between partitions | No guarantee (Kafka) | ⚠️ Standard Kafka behavior |

### Root Cause

This is an **architectural trade-off**, not a bug. Clusters are independent — no MirrorMaker, no Cluster Linking. Each cluster has its own data and offsets. When a partition shifts between clusters during failover, there is no data synchronization.

### Mitigation: Producer Idempotence

Enable idempotent producers to eliminate duplicates:

```properties
# Java / librdkafka
enable.idempotence=true
acks=all
max.in.flight.requests.per.connection=5
```

With idempotence enabled, Kafka assigns each producer a PID (Producer ID) and each message gets a sequence number. The broker deduplicates by `(PID, sequence)` — even if a message is produced twice, only one copy is persisted.

### Mitigation: Read Committed Isolation

Use transactional reads to skip duplicate messages:

```properties
# Consumer
isolation.level=read_committed
```

This ensures consumers only see committed messages from transactions. If a producer uses transactions (`transactional.id`), duplicate messages from non-transactional contexts may still appear.

### Mitigation: Application-Level Deduplication

For the most reliable deduplication, include a unique message ID in your payload and deduplicate at the application layer:

```json
{
  "message_id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2026-06-15T22:00:00Z",
  "payload": { ... }
}
```

### When Duplicates Are Acceptable

For many workloads, duplicates during the brief failover window are acceptable:
- **Log aggregation** — eventual consistency, duplicates filtered by timestamp
- **Metrics/telemetry** — last-write-wins, idempotent by nature
- **Notifications** — at-most-once delivery not critical
- **Event sourcing with idempotent handlers** — replay-safe by design

### When Duplicates Are Unacceptable

If duplicate messages are not acceptable, consider:
1. **Use a single cluster** (`mode: "single"`) — no failover, no partition migration
2. **Enable Cluster Linking** — keeps data and offsets synchronized across clusters
3. **Enable MirrorMaker 2** — replicates data with offset translation
4. **Use transactions** (`transactional.id` + `read_committed`) — atomic writes across partitions

### Ordering Guarantees Summary

| Guarantee | Normal | Failover | Failback |
|-----------|--------|----------|----------|
| Per-partition order | ✅ | ✅ (within Secondary) | ⚠️ Gap on Primary |
| No message loss | ✅ (acks=all) | ✅ (retry) | ⚠️ Duplicates |
| Exactly-once | ✅ (idempotent) | ✅ (retry) | ✅ (dedup) |
| Cross-partition order | ❌ | ❌ | ❌ |
