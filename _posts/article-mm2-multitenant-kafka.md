# MirrorMaker2 with Multi-Tenant Kafka: Architecture, Gotchas, and Real-World Behavior

> A deep dive into syncing tenant Kafka clusters to a central Kafka cluster using MirrorMaker2 with identity replication — the design choices, the failure modes, and the subtle traps nobody tells you about.

---

## The Problem: Multi-Tenant Kafka at Scale

Imagine you're running a platform for multiple business units or customers — each with their own isolated Kafka cluster (a "tenant" cluster). These tenants produce events independently. But your central data platform — responsible for analytics, archival, cross-tenant aggregations, or ML pipelines — needs to consume from all of them in one place.

This is the **multi-tenant Kafka fan-in problem.**

```
Tenant A Kafka ──┐
Tenant B Kafka ──┤──► Central Kafka ──► Data Platform (Analytics, ML, Archival)
Tenant C Kafka ──┘
```

One solution: **MirrorMaker2 (MM2)** — Kafka's built-in, Kafka Connect-based replication tool — deployed with **Identity Replication Policy**.

---

## What Is MirrorMaker2?

MirrorMaker2 (introduced in Kafka 2.4) is a multi-cluster replication tool built on top of Kafka Connect. It replicates topics from a source cluster to a target cluster, tracking offsets, syncing consumer group checkpoints, and managing heartbeat topics.

It runs as a set of Kafka Connect connectors:
- **MirrorSourceConnector** — replicates topic data
- **MirrorCheckpointConnector** — syncs consumer group offsets
- **MirrorHeartbeatConnector** — emits liveness signals

---

## Identity Replication Policy: What It Means

By default, MM2 uses `DefaultReplicationPolicy`, which renames topics at the destination:

```
source.my-topic  →  clusterA.my-topic
```

With **IdentityReplicationPolicy**, the topic name is preserved:

```
source.my-topic  →  my-topic
```

This matters when:
- Downstream consumers should not need to know which cluster a message originated from
- You want a single consumer configuration pointing at central Kafka to consume the same topic name regardless of source
- You're aggregating the same logical topic from multiple tenant clusters into one place

**Config:**
```properties
replication.policy.class=org.apache.kafka.connect.mirror.IdentityReplicationPolicy
```

---

## High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Central Kafka Cluster                         │
│   Topics: orders, payments, inventory  (aggregated from all tenants) │
└──────────────────────────────────────────────────────────────────────┘
         ▲              ▲              ▲
         │              │              │
    MM2 Instance   MM2 Instance   MM2 Instance
    (Tenant A)     (Tenant B)     (Tenant C)
         │              │              │
         ▼              ▼              ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ Tenant A     │ │ Tenant B     │ │ Tenant C     │
│ Kafka        │ │ Kafka        │ │ Kafka        │
│ orders       │ │ orders       │ │ orders       │
│ payments     │ │ payments     │ │ inventory    │
└──────────────┘ └──────────────┘ └──────────────┘
```

**Deployment choices:**
- One MM2 instance per tenant (isolated, easy to debug, scale independently)
- One shared MM2 instance connecting to all tenants (simpler ops, single point of failure)

Per-tenant MM2 instances are generally preferred for production — failure isolation is worth the extra overhead.

---

## Pros of This Approach

| Benefit | Detail |
|---|---|
| **Topic name transparency** | Consumers on central Kafka use the same topic name as tenant producers |
| **Decoupled clusters** | Tenant teams manage their Kafka independently; central team owns the hub |
| **Offset tracking** | MM2 syncs consumer group checkpoints so failover is possible |
| **Built-in Kafka ecosystem** | No external tools; MM2 is maintained by the Kafka project |
| **Scalable fan-in** | Add a new MM2 instance per tenant; central Kafka absorbs all streams |
| **At-least-once by default** | Reliable delivery without complex producer configuration on the source side |

---

## Cons and Real-World Challenges

| Challenge | Detail |
|---|---|
| **Fan-in topic collision** | With identity replication, all tenants writing to `orders` land in the same central `orders` topic — no source-tenant metadata unless producers embed it |
| **Partition count dependency** | Partition counts must be managed carefully (detailed below) |
| **Lag is eventual** | MM2 is an async replication layer; there is always some lag |
| **MM2 restart on config change** | Adding a new tenant or topic pattern may require MM2 restart |
| **No ordering guarantees across tenants** | Messages from Tenant A and B are interleaved in central Kafka |
| **Consumer group offset drift** | Offset sync is best-effort; not suitable as a zero-RPO failover mechanism |
| **Identity policy and internal topics** | MM2 internal heartbeat/checkpoint topics still get prefixed; only user data topics are identity-replicated |

---

## Replication Lag: What to Expect

MM2 introduces inherent replication lag because it is a consumer + producer pipeline, not a Kafka-native replication protocol.

**Lag sources:**
1. **Poll interval** — MM2 polls the source broker on a configurable interval (`consumer.poll.interval.ms`)
2. **Producer batching** — Messages are batched before being sent to the central cluster
3. **Network RTT** — Between tenant cluster and central cluster (especially across regions or clouds)
4. **Back-pressure** — If central Kafka is slow to accept writes (e.g., high load), MM2 producer waits

**Typical lag in a well-tuned same-region setup:** 100ms–2s  
**Typical lag across regions:** 2s–15s depending on network latency and throughput

**How to monitor lag:**
```bash
# MM2 exposes replication lag as a JMX metric
kafka.mirror:type=MirrorSourceConnector,target=central,topic=orders,partition=0
  → record-age-ms (age of the oldest unconsumed record at source)
  → record-count (number of records not yet replicated)
```

Or use `kafka-consumer-groups.sh --describe` against the source cluster to check MM2's consumer group lag.

---

## What Happens If MM2 Goes Down?

MM2 going down does **not** affect the tenant Kafka cluster or the central Kafka cluster. Both continue operating independently.

**What happens:**
- Tenant producers continue writing to their cluster normally
- Central Kafka consumers continue reading what has already been replicated
- MM2 stops replicating; lag accumulates at the source

**On MM2 restart:**
- MM2 reads its last committed offset from the source cluster (stored in the MM2 consumer group)
- It resumes from where it left off
- **No data loss** — all unconsumed messages are still in the source cluster (within the retention window)
- Lag catch-up happens automatically, usually quickly if the outage was short

**Risk: Extended outage beyond source retention window**
- If MM2 is down longer than the source cluster's retention period (e.g., 3 days), messages that expired will never be replicated
- This is a silent data gap — MM2 will resume from the earliest available offset, skipping the lost window

**Mitigation:** Monitor MM2 consumer lag continuously; alert if lag exceeds a threshold (e.g., > 1 hour of messages behind).

---

## What Happens When a New Tenant Is Added?

MM2 is configured with topic inclusion patterns, for example:

```properties
topics = .*
# or more specific:
topics = orders, payments, inventory
```

**Adding a new tenant cluster** (say Tenant D) requires:
1. A new MM2 instance (or adding a new source cluster to an existing MM2 config)
2. A config reload or restart of MM2

**Does it restart?** Yes — in most deployments, adding a new source cluster to MM2 requires a config update and restart of the MM2 process or the Kafka Connect cluster running MM2 connectors. There is no hot-add of a new source cluster without some downtime of the MM2 instance.

**New topic on an existing tenant:**  
If you add a new topic to an existing tenant Kafka and MM2's `topics` pattern matches it (e.g., `topics = .*`), MM2 will **automatically pick it up without a restart** — Kafka Connect dynamically discovers new topics at the next poll cycle.

---

## The Partition Count Trap

This is one of the most important — and least documented — gotchas with identity replication.

### Case 1: Tenant has FEWER partitions than Central

```
Tenant A: orders (4 partitions)
Central:   orders (8 partitions)
```

**What happens:** MM2 replicates messages from partitions 0–3 of the source. Partitions 4–7 on central will **never receive any data** from this tenant. They sit empty.

If another tenant (Tenant B) also has `orders` with 4 partitions, its MM2 instance will also only write to partitions 0–3 — mixing data from both tenants in those 4 partitions while 4–7 remain empty.

**This is a waste of partitions, not a data loss scenario.** Messages are not lost, just unevenly distributed.

### Case 2: Tenant has MORE partitions than Central ⚠️ DATA LOSS

```
Tenant A: orders (8 partitions)
Central:   orders (4 partitions)
```

**What happens:** MM2 uses a partition mapping function. With identity replication, it maps source partition N to target partition N. But if N >= 4 (target partition count), MM2 either:
- **Modulo maps** (partition 5 → partition 5 % 4 = 1), meaning multiple source partitions collapse into fewer target partitions — **ordering within a key may be violated**
- **Drops messages from out-of-range partitions** depending on MM2 version and configuration

In practice, **data loss or ordering violations** occur when the source has more partitions than the target. This is the dangerous scenario.

**Rule of thumb:** Always set `central Kafka topic partitions >= max(tenant topic partitions across all tenants)`.

### Safe Configuration Strategy

| Tenant | Topic | Partitions |
|---|---|---|
| Tenant A | orders | 4 |
| Tenant B | orders | 8 |
| Tenant C | orders | 12 |
| **Central** | **orders** | **≥ 12** |

When in doubt, pre-provision central Kafka topics with higher partition counts. You can always increase partitions later, but you cannot decrease them.

---

## Summary: Key Rules for Operating MM2 with Identity Replication

1. **Central partitions must be ≥ tenant partitions** for every topic, across all tenants. Enforce this via topic provisioning automation.
2. **MM2 outages are safe** as long as they don't outlast the source retention window. Monitor lag aggressively.
3. **Adding new tenants requires MM2 restart** (or a new MM2 instance per tenant). Plan for this operationally.
4. **New topics on existing tenants are auto-discovered** by MM2 without restarts if topic patterns match.
5. **Fan-in means topic collision** — embed tenant identifiers in message payloads or use separate topics per tenant on central Kafka if source attribution matters.
6. **Lag is always present** — MM2 is not synchronous replication. Size your SLAs accordingly.
7. **Offset sync is not failover-grade** — MM2 checkpoint sync helps but is not a reliable RPO-zero mechanism.
