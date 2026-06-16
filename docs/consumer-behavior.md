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
StickyHash(clientID, topic, partition) % 100

  hash < 70  →  Primary (70% traffic)
  hash >= 70 →  Secondary (30% traffic)
```

The same `(clientID, topic, partition)` tuple **always** routes to the same cluster. This is critical for correctness — a produce and subsequent fetch for the same partition must hit the same Kafka broker.

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
