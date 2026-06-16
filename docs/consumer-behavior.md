# Consumer Behavior with Load Balancing

## How Routing Works for Consumers

Bifrost routes **all** Kafka protocol requests — including Fetch (consume) — through the same routing logic:

| Request Type | API Key | Routed? | How |
|-------------|---------|---------|-----|
| Metadata | 3 | Yes | Synthetic merge from both clusters |
| Produce | 0 | Yes | Sticky hash: `(clientID, topic, partition)` |
| **Fetch** | **1** | **Yes** | **Sticky hash: `(clientID, topic, partition)`** |
| Offsets | 2 | Yes | Sticky hash |
| JoinGroup | 11 | Yes | Passthrough to primary |

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

### Important: Topic Mirroring Required

Both clusters **must have identical topic and partition layouts**. The synthetic metadata response presents a merged view from both clusters. If a partition exists on one cluster but not the other, consumers will get errors when fetching from the missing cluster.

```
✅ Primary:  orders (5 partitions) + Secondary:  orders (5 partitions)
❌ Primary:  orders (5 partitions) + Secondary:  orders (3 partitions)
❌ Primary:  orders (5 partitions) + Secondary:  NO orders topic
```

This is typically achieved through Confluent Cluster Linking, MirrorMaker 2, or multi-region topic replication.

## Consumer Group Coordination

Consumer group operations (JoinGroup, SyncGroup, Heartbeat) are forwarded to the **primary cluster only**. All offset commits go to the primary. This ensures:

- Consumer group state is consistent (managed by a single coordinator)
- Offset commits are reliable (stored on the primary)
- Rebalance decisions happen in one place

**The secondary is used only for consuming existing data**, not for group coordination.

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
T+15s   New Fetch for orders-0 → hash says "Primary" → Primary DOWN
        Router falls back to Secondary
T+15s   Consumer retries Fetch (internal Kafka client retry)
T+16s   Fetch succeeds on Secondary
```

The consumer may see brief errors (connection closed, timeout) during the detection window. Standard Kafka client retries handle this transparently.

### Consumer Lag During Failover

If the secondary cluster has replication lag, consumers may not see the most recent messages after failover. The `auto.offset.reset` policy determines behavior:

- `earliest`: Re-reads from the oldest available offset (may duplicate)
- `latest`: Skips to the newest (may miss messages)
- Use `enable.idempotence=true` on producers to handle potential duplicates

### Stickiness Change on Client Restart

If a consumer restarts and gets a new `client.id`, its sticky hash changes, potentially routing partitions to different clusters. This is expected behavior — the hash is per-client, not per-consumer-group.

## Summary

| Question | Answer |
|----------|--------|
| Are consumers load-balanced? | **Yes**, at the partition level via sticky hash |
| Does the consumer need to change? | **No** — it connects to one proxy address |
| Can one consumer fetch from both clusters? | **Yes** — if its partitions hash to both sides |
| Is consumer group coordination load-balanced? | **No** — always on Primary for consistency |
| Must topics be mirrored? | **Yes** — identical partition layouts required |
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
  p0 → Secondary   ← NOW routes here
  p1 → Secondary   [msg-4, msg-5]

Failback (70/30):
  p0 → Primary     ← returns here
```

**Problem:** Messages produced during failover for partition 0 live on Secondary, but after failback, consumers read from Primary — where those messages don't exist. Offsets committed during failover also live on Secondary. On failback, the consumer fetches offsets from Primary and may re-read messages already consumed from Secondary.

### Impact Summary

| Scenario | Behavior | Impact |
|----------|----------|--------|
| Normal operation | All p0 → Primary | ✅ Guaranteed ordering |
| During failover | p0 → Secondary | ⚠️ Data on different cluster |
| After failback | p0 → Primary | 🔴 Duplicates possible |
| Between partitions | No guarantee (Kafka) | ⚠️ Standard Kafka behavior |

### Root Cause

This is an **architectural trade-off**, not a bug. Clusters are independent — no MirrorMaker, no Cluster Linking. Each cluster has its own data and offsets. When a partition shifts between clusters, there is no data synchronization.

### Mitigation: Producer Idempotence

Enable idempotent producers to eliminate duplicates:

```properties
# Java / librdkafka
enable.idempotence=true
acks=all
max.in.flight.requests.per.connection=5
```

With idempotence enabled, Kafka assigns each producer a PID (Producer ID) and each message gets a sequence number. The broker deduplicates by `(PID, sequence)` — even if a message is produced twice (original on Primary, retry on Secondary after failover), only one copy is persisted.

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

