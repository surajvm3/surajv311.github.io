# Kafka Deep Dive: Architecture, Producer Configs, and Exactly-Once Semantics

> A practical deep dive into how Kafka works — from core primitives to multi-broker replication, producer durability configs, and exactly-once guarantees — grounded in a real-world GKE + Strimzi cluster running on Spot instances.

---

## The Cluster We're Working With

Before diving into theory, here's the real-world setup this article is grounded in:

| Config | Value |
|---|---|
| **Kafka version** | 4.3.1 (KRaft mode — no ZooKeeper) |
| **Brokers** | 3 |
| **Instance type** | Spot VMs, 1 broker per zone (3 zones) |
| **Replication factor** | 2 |
| **Retention** | 3 days |
| **Storage** | 220–250 GiB pd-balanced per broker (zonal) |
| **PodDisruptionBudget** | maxUnavailable: 1 |
| **Node pool** | Dedicated Spot pool, tainted, tolerations on Kafka pods |
| **Storage class** | pd-balanced (not local SSD) |

Three brokers, each pinned to a different GCP zone. Every partition has one leader and one follower — almost certainly in different zones.

---

## Part 1 — What Is Kafka, Really?

Think of Kafka as a **durable, ordered, high-throughput log on a network**. You write messages into it (produce), and other systems read from it (consume) at their own pace.

Unlike a message queue — where a message disappears after one consumer reads it — Kafka *keeps* the message for a configurable retention window. Multiple consumers can read the same message independently.

### The Core Primitives

**Topic**

A named stream of messages. Think of it like a table name or a named channel. Example: `robot.telemetry.battery`.

**Partition**

A topic is split into N ordered sub-logs called partitions. Each partition is an append-only file on disk. Messages within one partition are strictly ordered; across partitions, there is no ordering guarantee.

```
Topic: robot.telemetry.battery  (4 partitions)

Partition 0:  [msg1] [msg5] [msg9]  [msg13] ──► (append-only)
Partition 1:  [msg2] [msg6] [msg10] ──►
Partition 2:  [msg3] [msg7] [msg11] ──►
Partition 3:  [msg4] [msg8] [msg12] ──►
```

**Offset**

Every message in a partition has a monotonically increasing integer ID called an offset — like an array index. A consumer remembers "I've read up to offset 42 in partition 2" and picks up from there on restart.

**Broker**

A single Kafka server process. It stores partitions on disk, accepts produce requests, and serves fetch requests to consumers. In a cluster, each broker owns a subset of partitions.

**Producer**

Any process that writes messages to Kafka.

**Consumer Group**

A set of processes that together read a topic. Kafka assigns each partition to exactly one consumer in the group at a time — so the group reads every message exactly once as a whole, with individual consumers each handling a subset of partitions.

```
Consumer Group "analytics" reading topic with 4 partitions

  Consumer A ◄── Partition 0, Partition 1
  Consumer B ◄── Partition 2, Partition 3

  Add a third consumer → Kafka rebalances:
  Consumer A ◄── Partition 0
  Consumer B ◄── Partition 1
  Consumer C ◄── Partition 2, Partition 3
```

---

## Part 2 — Multi-Broker Cluster Architecture

A single broker is a single point of failure and a throughput ceiling. In production you run a cluster. Three reasons: **fault tolerance**, **throughput**, and **storage capacity**.

### Replication: How Data Survives Broker Failures

Every partition has one **leader** and zero-or-more **followers** (replicas). All produce and consume traffic goes to the leader. Followers pull from the leader to stay caught up.

```
Partition 0  (replication factor = 3)

  Broker 1 (LEADER) ──► Broker 2 (follower) ──► Broker 3 (follower)
       │                      │                       │
   writes go here       copies from leader      copies from leader
   reads go here

  If Broker 1 dies:
  Broker 2 or 3 is elected new leader → traffic shifts automatically
```

The set of followers that are fully caught up is called the **ISR — In-Sync Replicas**. If the leader dies, Kafka elects a new leader from the ISR. A replica that falls too far behind (controlled by `replica.lag.time.max.ms`) is removed from the ISR until it catches up.

### Partition Leadership Distribution

Kafka spreads partition leaders evenly across brokers. With 12 partitions and 3 brokers, each broker leads roughly 4 partitions — distributing both CPU and network load.

```
3-broker cluster, 12 partitions:

  Broker 1: leads P0, P3, P6, P9    (also follows P1,P2,P4,P5,P7,P8,P10,P11)
  Broker 2: leads P1, P4, P7, P10
  Broker 3: leads P2, P5, P8, P11
```

In our 3-broker, RF=2 setup, each partition has 1 leader and 1 follower:

```
Topic: orders  (4 partitions, replication factor: 2)

Partition 0:  Leader → Broker 1 (zone-a)  |  Replica → Broker 2 (zone-b)
Partition 1:  Leader → Broker 2 (zone-b)  |  Replica → Broker 3 (zone-c)
Partition 2:  Leader → Broker 3 (zone-c)  |  Replica → Broker 1 (zone-a)
Partition 3:  Leader → Broker 1 (zone-a)  |  Replica → Broker 3 (zone-c)
```

### The Controller Broker

One broker is elected **controller**. It has extra duties: tracking which brokers are alive, re-assigning partition leadership when a broker dies, and processing topic creation/deletion. It is still an ordinary broker — just with added responsibilities. If it fails, another broker takes over via a fast election.

### ZooKeeper vs KRaft

Historically Kafka used **ZooKeeper** — a separate cluster — to store metadata: which broker is controller, which partitions have which leaders, ISR lists, etc. This meant running and maintaining a separate ZooKeeper ensemble alongside every Kafka cluster.

Modern Kafka (3.x+) replaces this with **KRaft** — Kafka's own Raft-based consensus built directly in. Brokers elect a controller amongst themselves. No external dependency, simpler operations, and faster metadata operations. Our cluster runs 4.3.1 in KRaft mode — no ZooKeeper anywhere.

### A Produce Request's Journey Through a Cluster

```
1. Producer asks any broker: "who leads partition 2 of topic battery.telemetry?"
2. Broker responds: "Broker 3 leads that partition"
3. Producer connects directly to Broker 3, sends the compressed batch
4. Broker 3 writes the batch to its local log
5. Broker 1, Broker 2 (followers) pull the batch and write to their local logs
6. Once ISR quorum acks, Broker 3 replies to producer: "OK, offset 10042"

Producer ──► Broker 3 (leader)
                  │
                  ├──► Broker 1 (follower, acks)
                  └──► Broker 2 (follower, acks)
                  │
                  └──► "OK, offset 10042" ──► Producer
```

---

## Part 3 — Producer Acknowledgement: The acks Setting

When a producer sends a record, it can ask Kafka for different levels of confirmation before considering the write "done." This is the `acks` setting — the single most important knob for balancing throughput vs. durability.

### acks=0: Fire and Forget

```
Producer ──► Broker (no wait)  ──► socket buffer
                                    (message may or may not persist)
```

The producer sends the message and **does not wait for any acknowledgement** from the broker. As soon as the message hits the network socket buffer, the producer considers it sent.

- Kafka may or may not have written it to disk
- If the broker crashes between receiving and persisting, the message is gone
- The producer has no idea whether it was received
- Returned offset is always `-1` (meaningless)
- **Retries do nothing** — the producer can't know what failed

**When to use:** Metrics, logs, or telemetry where occasional loss is acceptable and maximum throughput is the goal. Never for financial, transactional, or auditable data.

**Throughput:** Maximum possible — no network roundtrip for acks.

### acks=1: Leader Acknowledgement Only

```
Producer ──► Leader Broker
                │
                ├── Writes to leader's local log
                ├── Sends ACK back to producer  ◄── producer considers it done here
                │
                └── (asynchronously) followers pull from leader
```

The leader writes the record to its local log and immediately acknowledges the producer. It does **not** wait for followers to replicate.

**The risk — "leader fails after ack, before follower replicates":**

```
t=0: Producer sends message M
t=1: Leader (Broker 1, zone-a) writes M to local log, sends ACK ✓
t=2: Producer considers M "delivered"
t=3: Follower (Broker 2, zone-b) hasn't pulled M yet
t=4: Broker 1 Spot VM is evicted ← M is LOST
t=5: Broker 2 becomes new leader — it has no record of M
```

On Spot VMs, evictions are sudden — the VM can vanish with little warning. With RF=2 and acks=1, this window is real.

**When to use:** Medium-criticality streams where some data loss is tolerable but throughput matters.

**Throughput:** High — one network roundtrip, no follower coordination.

### acks=all (or acks=-1): Full ISR Acknowledgement

```
Producer ──► Leader Broker
                │
                ├── Writes to leader's local log
                ├── Waits for all ISR replicas to acknowledge
                │       │
                │       └── Follower pulls from leader, writes, sends ack to leader
                │
                └── Sends ACK back to producer  ◄── producer considers it done here
```

The leader waits until **all in-sync replicas** have written the record before acknowledging the producer. This is the strongest durability guarantee Kafka offers.

In our 3-broker, RF=2 cluster: ISR = `{leader, 1 follower}`. So `acks=all` means the record is on 2 brokers before the producer gets an ACK. Even if one broker is evicted immediately after, the message is safe on the surviving broker.

**The critical companion — min.insync.replicas:**

```properties
min.insync.replicas=2
```

This sets the minimum ISR size required for a write to succeed. If the ISR shrinks below this value (e.g., the follower is evicted), Kafka refuses new writes with `NotEnoughReplicasException`.

Without this guard: if ISR = `{leader only}`, `acks=all` would only wait for the leader — which completely defeats the purpose.

**Durability matrix for our RF=2 cluster:**

| acks | min.insync.replicas | Copies before ACK | Survives leader eviction? |
|---|---|---|---|
| 0 | any | 0 | No |
| 1 | any | 1 | No |
| all | 1 | 1 | No |
| all | 2 | 2 | **Yes** |

> **Rule of thumb:** `replication.factor=3` with `min.insync.replicas=2` is the standard production setup. It tolerates one broker failure without compromising durability or blocking writes.

**When to use:** Any data where loss is unacceptable. Required for idempotent and exactly-once producers.

**Throughput:** Lower than acks=1 — the roundtrip includes follower replication latency. In a same-region multi-zone cluster, this is typically 5–20ms added latency.

---

## Part 4 — Producer Configs Explained

Beyond `acks`, several other producer configs have significant impact on throughput, latency, memory, and reliability.

### RequiredAcks — Covered above

Use `acks=all`. Non-negotiable for durable systems.

### batch.size and linger.ms — Throughput Tuning

The producer accumulates records into batches before sending. Two configs control when a batch is flushed:

- **`batch.size`** — maximum bytes in a single produce batch. Once a batch hits this size, it is sent immediately.
- **`linger.ms`** — how long the producer waits to fill a batch before sending even if it isn't full yet. Default is `0` (send immediately).

```
linger.ms=0:
  Record arrives → batch sent immediately (low latency, low throughput)

linger.ms=10:
  Record arrives → wait up to 10ms → more records accumulate → larger batch sent
  (slightly higher latency, dramatically better throughput)
```

Setting `linger.ms` to 5–20ms dramatically improves throughput on high-volume topics by allowing more records to accumulate per batch, at the cost of tiny and usually imperceptible latency increases. This is one of the most impactful and least disruptive Kafka producer tuning levers.

### max.in.flight.requests.per.connection — Parallelism vs. Ordering

How many produce requests can be in-flight simultaneously to one broker. More in-flight requests = more parallelism = higher throughput. But there is a catch:

Without idempotence: if you send batches A and B, A fails and retries after B succeeds, the partition log ends up with B before A — **ordering is violated**.

With the **idempotent producer**, this is capped at **5** — Kafka's enforced maximum that still maintains ordering via sequence numbers. You get parallelism without reordering risk.

```properties
# With idempotent producer (required):
max.in.flight.requests.per.connection=5   # max allowed; kafka enforces ordering via seq numbers

# Without idempotence (higher throughput, ordering risk):
max.in.flight.requests.per.connection=10+  # reordering possible on retry
```

### ProduceRequestTimeout — Single RPC Timeout

How long the producer waits for a response to a single produce RPC. If the broker does not respond in this window, the request fails and is retried.

Think of it as: *"how patient am I with one individual network call?"*

```properties
request.timeout.ms=5000   # 5 seconds per RPC attempt
```

Too low → spurious timeouts under transient load spikes trigger unnecessary retries.
Too high → a stuck broker ties up the producer for a long time before retrying.

### RecordDeliveryTimeout — Total Retry Budget per Record

Kafka clients retry failed batches automatically. `delivery.timeout.ms` is the total wall-clock budget for one record — from first attempt to final delivery across all retries.

```
t=0:00  Record enqueued, Kafka unreachable
t=0:05  Retry 1 → still unreachable (RPC timeout: 5s)
t=0:15  Retry 2 → still unreachable
t=0:30  Retry 3 → ...
...
t=2:00  delivery.timeout.ms expires → ErrRecordTimeout
        → record is dropped, callback fires with error
```

```properties
delivery.timeout.ms=120000   # 2 minutes total retry window
```

The relationship between the two:
- `request.timeout.ms` = timeout for **one attempt**
- `delivery.timeout.ms` = total budget across **all attempts**
- `delivery.timeout.ms` must be ≥ `request.timeout.ms`

### MaxBufferedRecords — In-Memory Cap

The producer keeps an internal in-memory buffer of records waiting to be batched and sent. `max.buffered.records` (or equivalent in your client library) caps how many can sit in that buffer.

When the buffer is full (e.g., Kafka is unreachable and records pile up), new produce attempts are rejected immediately — no blocking, no OOM.

```
HTTP request → produce() → [internal buffer, max N records]
                                    │
                                    └──► batcher → broker → ack

If buffer fills (Kafka unreachable):
  produce() → ErrMaxBuffered → caller handles the drop
```

Without a cap, a slow or dead broker causes the producer to accumulate records in memory until the process OOMs. The cap trades *some* data loss for process stability — an acceptable trade-off for most systems.

**Sensible sizing:**

| Bound | Value | Reason |
|---|---|---|
| Too low | < 5,000 | Buffer drains too fast under normal bursts |
| Reasonable default | 50,000 | Safe starting point for moderate write rates |
| Upper guard | 500,000 | Beyond this, memory pressure becomes real |

### ProducerBatchCompression — Compress Before Sending

```properties
compression.type=zstd   # on the producer
```

The producer compresses entire **batches** (not individual messages) before sending to the broker. **zstd** is a strong choice because:

- Line-protocol or JSON data is highly repetitive text — field names, measurement names, tags repeat constantly. Compression ratios of 5x–10x are typical.
- zstd achieves better ratios than gzip/snappy at similar CPU cost.
- Compressing at batch level amortises the CPU cost across many records per batch.

**Critical: also set `compression.type=producer` on the topic:**

```properties
# Topic config (set at topic creation or via admin API)
compression.type=producer
```

This tells the broker: *"store batches exactly as I sent them, do not recompress."* Without this, the broker may decompress your zstd batch and re-compress with the cluster default codec — wasting CPU on both ends and potentially changing the wire format for downstream consumers.

---

## Part 5 — The Three Delivery Guarantees

Every messaging system makes one of three promises about delivery. It is worth naming them clearly before going into exactly-once mechanics.

| Guarantee | What It Means | Risk |
|---|---|---|
| **At-most-once** | A message might be lost. Never delivered twice. | Data loss |
| **At-least-once** | Eventually delivered. Might be delivered more than once. | Duplicates |
| **Exactly-once** | Delivered exactly once. No loss, no duplicates. | Requires deliberate config |

Most Kafka producers default to **at-least-once** — they retry on failure, which is correct, but that retry can produce duplicates if the broker already wrote the message but the ack was lost.

---

## Part 6 — Idempotent Producer: Eliminating Duplicates on Retry

Even with `acks=all`, there is a subtle problem: **producer retries create duplicates.**

```
Producer                    Broker
   │                            │
   ├──── Batch (seq=0) ────────►│  broker writes to log, prepares ack
   │                             │
   │         [network blip]      │
   │◄──── (ack never arrives)    │
   │                             │
   │  "I didn't get an ack,      │
   │   I'll retry"               │
   ├──── Batch (seq=0) ────────►│  ← broker writes it AGAIN (duplicate!)
   │◄──── "OK" ─────────────────│
```

The producer cannot distinguish "ack was lost" from "write failed." So it retries. The broker, without idempotence, has no memory of the first write. You get a duplicate record.

### How the Idempotent Producer Solves This

Enable via `enable.idempotence=true` (modern clients enable this by default when `acks=all`):

1. The broker assigns the producer a **Producer ID (PID)** — a unique integer for this producer instance's lifetime.
2. Each partition gets its own monotonically increasing **sequence number**.
3. Every batch is stamped with `(PID, partition, sequence_number)`.
4. If the broker receives a batch with a sequence number it has already written, it accepts the request (returns success) but **silently discards the duplicate**.

```
Producer (PID = 42)              Broker
     │                               │
     ├── Batch (seq=0) ─────────────►│  written; seq=0 recorded for PID 42
     │          [ack lost]            │
     ├── Batch (seq=0) ─────────────►│  "already have seq=0 for PID 42 — discard"
     │◄────── "OK" ──────────────────│  producer thinks it succeeded (it did, the first time)
     │                               │
     ├── Batch (seq=1) ─────────────►│  written; seq=1 recorded
     │◄────── "OK" ──────────────────│
```

No duplicate, even though a retry occurred.

**Idempotence requirements:**

```properties
enable.idempotence=true
acks=all                                       # required — kafka enforces this
max.in.flight.requests.per.connection=5        # max allowed with idempotence
retries=2147483647                             # must be > 0
```

### The Important Caveat: PID Resets on Producer Restart

The PID is assigned fresh on each producer startup. If the process crashes and a new instance starts, it gets a new PID. The broker will not recognise retry attempts from the new instance as duplicates of the old one.

A record that was in-flight at crash time could be written twice — once by the old instance (before crash) and once by the new instance (on startup retry).

For most telemetry and event-streaming workloads, a single duplicate point on a process restart is acceptable — it is a known, bounded limitation of the idempotent producer without full transactions.

---

## Part 7 — Exactly-Once Semantics (EOS): The Full Picture

Idempotent producer gives you exactly-once *from producer to broker* within a single session. It does **not** give you:

- Exactly-once across multiple topics simultaneously
- Exactly-once from broker to consumer (the consumer commits its own offsets separately)
- End-to-end exactly-once across the full pipeline (producer → Kafka → consumer → database)
- Exactly-once across producer restarts (PID resets)

For those, you need **Kafka Transactions**.

### Transactions: Atomic Multi-Partition Writes

```
Producer:
  beginTransaction()
    write(topic=A, partition=0, message=M1)
    write(topic=B, partition=2, message=M2)
  commitTransaction()   ← both M1 and M2 become visible atomically
  # or
  abortTransaction()    ← neither M1 nor M2 becomes visible
```

A **transactional producer** is assigned a stable `transactional.id` that persists across restarts. When it restarts, it first resolves any in-flight transaction from the previous session (commit or abort), then starts fresh — no duplicates across restarts.

**Producer config for full EOS:**

```properties
enable.idempotence=true
transactional.id=my-unique-producer-id    # stable across restarts
acks=all
```

**Consumer config — only read committed messages:**

```properties
isolation.level=read_committed
```

Without `read_committed`, consumers see messages from uncommitted (in-flight) transactions — which may later be aborted, causing **phantom reads**: the consumer processes a message that was never actually committed.

---

## Part 8 — Other Important Configs Every Kafka Engineer Should Know

### Producer Side

**`linger.ms`** — Covered above. Set to 5–20ms for throughput-heavy topics.

**`batch.size`** — Maximum bytes per batch. Tune alongside `linger.ms`. Larger batches = better throughput and compression, slightly more latency.

**`transactional.id`** — If set, enables the transactional producer for full exactly-once across multiple partitions and producer restarts.

**`max.in.flight.requests.per.connection`** — Capped at 5 with idempotence. Without idempotence, can be higher for throughput but risks reordering on retry.

### Broker / Topic Side

**`min.insync.replicas`** — Minimum ISR size for `acks=all` to succeed. Set to 2 when replication factor is 3. This is the guard that makes `acks=all` meaningful.

**`retention.ms`** — How long messages are kept before old segments are deleted (default: 7 days). For pipelines where Kafka is a delivery buffer into a downstream store, shorter retention is fine — the data lands quickly and long retention just wastes disk.

**`retention.bytes`** — Cap retention by size instead of (or in addition to) time. Useful for predictable capacity planning on bounded disk.

**`compression.type` (topic-level)** — Set to `producer` to preserve the producer's compression codec. Any other value causes the broker to decompress and recompress — wasting CPU.

### Consumer Side

**`auto.offset.reset`** — What to do when a consumer group has no committed offset yet:
- `earliest` — start from the beginning of the log
- `latest` — start from now, skip all historical data

Wrong choice here silently loses your entire backlog. In most production pipelines, `earliest` is safer.

**`isolation.level`** — `read_committed` shows only committed transaction data. `read_uncommitted` (the default) shows everything including in-flight transactions that may later be aborted.

**`enable.auto.commit`** — Whether the client automatically commits offsets on a timer. Disabling this and committing manually — *after* your processing is confirmed successful — gives you exactly-once semantics on the consumer side. Auto-commit risks committing an offset for a message your processing failed on.

---

## Part 9 — Putting It Together: Our Spot VM Cluster

Running Kafka on Spot VMs is economical but introduces real eviction risk.

```
Normal operation (all 3 brokers healthy):
  RF=2, ISR={broker-1, broker-2}
  acks=all → waits for 2 copies → safe

One broker evicted (Spot eviction in zone-a):
  RF=2, ISR shrinks to {leader only}
  If min.insync.replicas=2 → writes BLOCK until follower catches up or broker returns
  This is the right behavior — better to pause than to lose data silently

PodDisruptionBudget: maxUnavailable=1:
  GKE will not evict more than 1 broker pod at once during planned maintenance
  Spot evictions bypass this — the cloud can reclaim the VM regardless
  ⚠️  This is why RF=2 on Spot is risky for zero-write-pause guarantees
      RF=3 would be safer: ISR can tolerate 1 loss and still have 2 members
```

**Recommended config for production on Spot + RF=2:**

```properties
# Producer
acks=all
enable.idempotence=true
retries=2147483647
max.in.flight.requests.per.connection=5
request.timeout.ms=5000
delivery.timeout.ms=120000
linger.ms=10
compression.type=zstd

# Topic / Broker
min.insync.replicas=2
compression.type=producer

# Consumer (for EOS pipelines)
isolation.level=read_committed
enable.auto.commit=false
auto.offset.reset=earliest
```

**Trade-off:** With RF=2 and `min.insync.replicas=2`, losing one broker pauses writes until it recovers. With RF=3 and `min.insync.replicas=2`, losing one broker is fully transparent — the ISR still has 2 members and writes continue uninterrupted. On Spot-based clusters, RF=3 is the safer choice if write availability matters.

---

## Part 10 — What Happens When Kafka Goes Down?

A producer does not fail immediately when a broker becomes unreachable. Retries and buffering absorb short outages transparently.

```
t=0:00  Kafka becomes unreachable
        produce() still enqueues records into in-memory buffer (broker issue ≠ enqueue failure)

t=0:05  First produce attempt fails → queued for retry (with backoff)
t=0:30  Retries continue; buffer fills slowly as new records arrive
...
t=2:00  delivery.timeout.ms expires → undelivered records get ErrRecordTimeout
        → callback fires with error; records are dropped; metrics can count these

If buffer fills before delivery.timeout.ms:
        → ErrMaxBuffered → new records rejected at enqueue; older records still retrying

t=2:30  Kafka comes back
        Producer reconnects automatically, drains buffered records, catches up
```

**The silent danger — outage longer than retention:**
If Kafka is unreachable for longer than the source retention window, buffered but undelivered records expire on the producer side. On reconnect, Kafka cannot replay what the producer dropped — those records are permanently lost. Monitor producer lag and delivery error metrics continuously and alert before the window closes.

---

## Summary

| Config | What It Controls |
|---|---|
| `acks=all` | Leader waits for all ISR replicas to confirm before acking producer |
| `min.insync.replicas=2` | Guard: write fails if ISR drops below 2 (makes acks=all meaningful) |
| `enable.idempotence=true` | Broker deduplicates retries via PID + sequence number |
| `transactional.id` | Stable producer identity across restarts; enables atomic multi-partition writes |
| `isolation.level=read_committed` | Consumer only sees committed transaction data, no phantom reads |
| `enable.auto.commit=false` | Manual offset commit after confirmed processing = consumer-side EOS |
| `linger.ms` | Batch accumulation window; trade tiny latency for big throughput gains |
| `max.in.flight.requests.per.connection=5` | Parallelism cap that preserves ordering with idempotence |
| `request.timeout.ms` | Single RPC attempt timeout |
| `delivery.timeout.ms` | Total retry window per record across all attempts |
| `compression.type=zstd` (producer) | Compress batches before sending; 5–10x ratio on text data |
| `compression.type=producer` (topic) | Broker stores as-is; prevents wasteful decompress + recompress |

**Key insight — acks=all alone is not enough:** Without `min.insync.replicas=2`, `acks=all` can silently degrade to single-copy durability when replicas fall out of the ISR. Always configure both together.

**Key insight — idempotence covers one session:** The PID resets on producer restart. For cross-restart exactly-once guarantees, you need `transactional.id`.

**Key insight — linger.ms is underused:** The default of `0` is optimised for latency, not throughput. Setting it to 5–20ms on high-volume topics is one of the highest-ROI Kafka tuning changes you can make.
