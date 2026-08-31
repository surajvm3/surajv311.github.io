---
layout: post 
title: Syncing Kafkas - MM2 
category: technicalArticles
---

> From my experience working at [GreyOrange](https://www.greyorange.com/). Refactored my article a bit with help of GPT. 

I was working on a problem statement to sync data from multi-tenant kafkas to central kafka. Upon research, I came across MirrorMaker2. 

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

MirrorMaker2 (introduced in Kafka 2.4) is a multi-cluster replication tool built on top of Kafka Connect. It replicates topics from a source cluster to a target cluster, tracking offsets, syncing consumer group checkpoints, and managing heartbeat topics.

It runs as a set of Kafka Connect connectors:
- **MirrorSourceConnector** — replicates topic data
- **MirrorCheckpointConnector** — syncs consumer group offsets
- **MirrorHeartbeatConnector** — emits liveness signals

By default, MM2 uses `DefaultReplicationPolicy`, which renames topics at the destination:

```
source.my-topic  →  clusterA.my-topic
```

With **IdentityReplicationPolicy**, the topic name is preserved (which we used in our setup):

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

Now, I have been working on testing MM2 (GCP managed not opensource). Multiple mm2 instances/connectors can be added in the GCP managed kafka connect cluster. Below are some of the scenarios and their answers, hopefully when I scale things and encounter more scenarios I will update the article. 

- MM2 introduces inherent replication lag because it is a consumer + producer pipeline. Although for smaller traffic, I did not observe this. 
- MM2 going down does not affect the tenant Kafka cluster or the central Kafka cluster. Both continue operating independently.
- On MM2 restart: MM2 reads its last committed offset from the source cluster (stored in the MM2 consumer group). It resumes from where it left off. Data is not lost if it was within rentention window of source kafka. 
- When a new tenant kafka is added, we can add a new MM2 instance in the cluster (cluster doesn't restart). Topic patterns are matched based on config, and they sync.
- Partition count scenarios:
  - Case 1: Tenant has FEWER partitions than Central: MM2 replicates messages from partitions 0–3 of the source. Partitions 4–7 on central will not receive any data from this tenant. They sit empty. If another tenant (Tenant B) also has `orders` with 4 partitions, its MM2 instance will also only write to partitions 0–3 — mixing data from both tenants in those 4 partitions while 4–7 remain empty. Messages are not lost, just unevenly distributed.
    ```
    Tenant A: orders (4 partitions)
    Central:  orders (8 partitions)
    ```
    
  - Case 2: Tenant has MORE partitions than Central: Modulo maps (partition 5 → partition 5 % 4 = 1), meaning multiple source partitions collapse into fewer target partitions
    ```
    Tenant A: orders (8 partitions)
    Central:   orders (4 partitions)
    ```

------------------------------

