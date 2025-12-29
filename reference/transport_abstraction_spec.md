# Transport Abstraction Specification

## Overview

A pluggable transport layer in `hs-rustlib` that allows dfe-loader-clickhouse to receive
messages from multiple sources without code changes. The transport is **payload-agnostic** -
it delivers raw bytes (JSON or MsgPack) without any envelope or framing.

**Location:** `hs-rustlib/src/transport/`

---

## Design Principles

1. **No envelope format** - Payloads are raw JSON/MsgPack, unchanged
2. **Vector.dev compatible** - Kafka topics work with Vector's `json` codec
3. **Transport metadata in tokens** - Offsets/positions never in payload
4. **Format detection at receiver** - Auto-detect JSON vs MsgPack
5. **Zero-copy where possible** - Minimal allocations in hot path

---

## Transport Implementations

| Transport | Use Case | Backend | Features |
|-----------|----------|---------|----------|
| `KafkaTransport` | Production | rdkafka | Consumer groups, at-least-once |
| `ZenohTransport` | Dev/test + production | zenoh | SHM same-node, TCP cross-node, mesh |
| `MemoryTransport` | Unit tests | tokio channel | In-process, zero overhead |

---

## Zenoh Mesh Topology

Zenoh supports multiple deployment patterns. For K8s, we use **routed mesh**.

### Same-Node Communication (Shared Memory)

When producer and loader pods are on the same K8s node, Zenoh automatically uses
POSIX shared memory for ~30µs latency with zero network overhead.

```
┌─────────────────────────────────────────────────────────────┐
│  K8s Node                                                   │
│                                                             │
│  ┌─────────────────┐    /dev/shm     ┌──────────────────┐  │
│  │  Producer Pod   │◄───────────────►│  Loader Pod      │  │
│  │  (zenoh peer)   │   zero-copy     │  (zenoh peer)    │  │
│  └─────────────────┘                 └──────────────────┘  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Cross-Node Communication (TCP Mesh)

When pods are on different nodes, Zenoh uses TCP. Two patterns:

#### Pattern A: Peer-to-Peer (Simple, <10 nodes)

Peers discover each other via multicast or configured endpoints.

```
┌──────────────────┐         TCP          ┌──────────────────┐
│  Node 1          │◄────────────────────►│  Node 2          │
│  ┌────────────┐  │                      │  ┌────────────┐  │
│  │ Producer   │  │                      │  │ Loader     │  │
│  │ (peer)     │  │                      │  │ (peer)     │  │
│  └────────────┘  │                      │  └────────────┘  │
└──────────────────┘                      └──────────────────┘
```

**Config:**
```toml
[zenoh]
mode = "peer"
connect = ["tcp/loader-service:7447"]
```

#### Pattern B: Routed Mesh (Production, >10 nodes)

Zenoh routers handle discovery and routing. Routers can be DaemonSet or dedicated pods.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Zenoh Router Mesh                           │
│                                                                     │
│    ┌──────────────┐       TCP        ┌──────────────┐              │
│    │ Router Pod   │◄────────────────►│ Router Pod   │              │
│    │ (Node 1)     │                  │ (Node 2)     │              │
│    └──────┬───────┘                  └──────┬───────┘              │
│           │ SHM/TCP                         │ SHM/TCP              │
│    ┌──────▼───────┐                  ┌──────▼───────┐              │
│    │ Producer     │                  │ Loader       │              │
│    │ (client)     │                  │ (client)     │              │
│    └──────────────┘                  └──────────────┘              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

**Router DaemonSet benefits:**
- Clients only connect to local router (localhost or SHM)
- Routers handle cross-node routing
- Automatic failover if a router dies
- Reduced connection count (N routers vs N² peers)

**Config (client):**
```toml
[zenoh]
mode = "client"
connect = ["tcp/localhost:7447"]  # Local router
```

**Config (router):**
```toml
[zenoh]
mode = "router"
listen = ["tcp/0.0.0.0:7447"]
connect = ["tcp/router-1:7447", "tcp/router-2:7447"]  # Other routers
```

### K8s Deployment Example

```yaml
# Router as DaemonSet (one per node)
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: zenoh-router
spec:
  template:
    spec:
      hostNetwork: true  # For SHM with pods
      containers:
      - name: zenoh
        image: eclipse/zenoh:latest
        args: ["--config", "/etc/zenoh/config.json5"]
        ports:
        - containerPort: 7447
          hostPort: 7447
---
# Loader connects to local router
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dfe-loader
spec:
  template:
    spec:
      containers:
      - name: loader
        env:
        - name: TRANSPORT_TYPE
          value: "zenoh"
        - name: ZENOH_CONNECT
          value: "tcp/localhost:7447"
```

---

## Trait Definition

```rust
// hs-rustlib/src/transport/mod.rs

use std::sync::Arc;
use async_trait::async_trait;

/// Transport-specific token for commit/acknowledgment
pub trait CommitToken: Send + Sync + Clone + 'static {
    /// Serialize for logging/debugging
    fn to_string(&self) -> String;
}

/// A received message with transport metadata
pub struct Message<T: CommitToken> {
    /// Routing key (Kafka topic, Zenoh key expression)
    pub key: Option<Arc<str>>,
    /// Raw payload bytes - JSON or MsgPack, unchanged
    pub payload: Vec<u8>,
    /// Transport-specific commit token
    pub token: T,
    /// Message timestamp from transport layer
    pub timestamp_ms: Option<i64>,
}

/// Backpressure signal from send operations
pub enum SendResult {
    /// Message accepted
    Ok,
    /// Transport is backpressured, retry later
    Backpressured,
    /// Fatal error, cannot continue
    Fatal(TransportError),
}

/// Transport-agnostic message delivery
#[async_trait]
pub trait Transport: Send + Sync {
    type Token: CommitToken;

    /// Send raw bytes to a key/topic
    async fn send(&self, key: &str, payload: &[u8]) -> SendResult;

    /// Receive up to `max` messages
    async fn recv(&self, max: usize) -> Result<Vec<Message<Self::Token>>, TransportError>;

    /// Commit/acknowledge processed messages
    async fn commit(&self, tokens: &[Self::Token]) -> Result<(), TransportError>;

    /// Shutdown gracefully
    async fn close(&self) -> Result<(), TransportError>;
}
```

---

## Token Types

```rust
// Kafka
#[derive(Clone)]
pub struct KafkaToken {
    pub topic: Arc<str>,
    pub partition: i32,
    pub offset: i64,
}

// Zenoh
#[derive(Clone)]
pub struct ZenohToken {
    pub key_expr: Arc<str>,
    pub timestamp: Option<u64>,  // Zenoh HLC timestamp
}

// Memory (for tests)
#[derive(Clone)]
pub struct MemoryToken {
    pub seq: u64,
}
```

---

## Module Structure

```
hs-rustlib/src/transport/
├── mod.rs              # Trait definitions, re-exports
├── error.rs            # TransportError, SendResult
├── kafka/
│   ├── mod.rs          # KafkaTransport implementation
│   ├── config.rs       # KafkaConfig (wraps existing)
│   └── token.rs        # KafkaToken
├── zenoh/
│   ├── mod.rs          # ZenohTransport implementation
│   ├── config.rs       # ZenohConfig
│   └── token.rs        # ZenohToken
└── memory/
    ├── mod.rs          # MemoryTransport (for tests)
    └── token.rs        # MemoryToken
```

---

## Feature Flags

```toml
# hs-rustlib/Cargo.toml

[features]
transport = []                              # Core trait only
transport-kafka = ["transport", "rdkafka"]  # Kafka adapter
transport-zenoh = ["transport", "zenoh"]    # Zenoh adapter
transport-memory = ["transport"]            # In-memory (no deps)
```

---

## Integration with dfe-loader-clickhouse

### Config

```toml
# config.toml
[transport]
type = "kafka"  # or "zenoh" or "memory"

[transport.kafka]
brokers = ["localhost:9092"]
group = "dfe-loader"
topics = ["events"]

[transport.zenoh]
mode = "peer"  # or "client"
connect = ["tcp/localhost:7447"]
subscribe = ["events/**"]
```

### Orchestrator Changes

```rust
// Before: Kafka-specific
let consumer = Consumer::new(&config.kafka)?;

// After: Transport-agnostic
let transport: Box<dyn Transport<Token = _>> = match &config.transport.transport_type {
    TransportType::Kafka => Box::new(KafkaTransport::new(&config.transport.kafka)?),
    TransportType::Zenoh => Box::new(ZenohTransport::new(&config.transport.zenoh)?),
    TransportType::Memory => Box::new(MemoryTransport::new()),
};
```

---

## Reliability and At-Least-Once Semantics

### The Critical Difference

| Transport | At-Least-Once | Persistence | Replay After Crash |
|-----------|---------------|-------------|-------------------|
| **Kafka** | ✅ Native | ✅ Broker disk | ✅ Consumer groups |
| **Zenoh** | ⚠️ In-flight only | ❌ Memory only | ❌ No replay |
| **Memory** | ❌ None | ❌ None | ❌ No replay |

### How Zenoh Reliability Works

Zenoh provides **reliable delivery for in-flight messages** but **not persistence**:

1. **Subscriber-side config**: Receivers choose `Reliable` or `BestEffort`
2. **Congestion control**: Publishers choose `Block` (backpressure) or `Drop`
3. **Hop-to-hop ACKs**: Messages retransmitted if lost in transit
4. **No disk WAL**: If a node crashes, in-flight messages are lost

From [Zenoh Reliability Blog](https://zenoh.io/blog/2021-06-14-zenoh-reliability/):
> "Senders and intermediate infrastructure components individually decide how much memory
> they are willing to dedicate to reliability"

### Storage Backends (Not for Streaming)

Zenoh has storage backends (RocksDB, Filesystem, S3, InfluxDB) but these are for
**queryable data-at-rest**, not streaming durability:

- Store key-value pairs for later query
- Not a WAL for message replay
- Not suitable for at-least-once streaming

### Implications for dfe-loader

| Scenario | Kafka | Zenoh |
|----------|-------|-------|
| Loader crashes mid-batch | ✅ Replay from uncommitted offset | ❌ Messages lost |
| ClickHouse down temporarily | ✅ Messages buffered in broker | ⚠️ Backpressure to sender |
| Network partition | ✅ Broker retains messages | ❌ Messages may be lost |

### Recommended Usage

| Use Case | Transport | Why |
|----------|-----------|-----|
| Production (critical data) | **Kafka** | At-least-once with replay |
| Dev/test | **Zenoh** | No broker, fast iteration |
| Same-node high-throughput | **Zenoh (SHM)** | 30µs latency, zero-copy |
| Staging (acceptable loss) | **Zenoh** | Simpler ops than Kafka |

### Hybrid Pattern (Future)

For production without Kafka, add sender-side WAL:

```
Producer → [WAL] → Zenoh → Loader → ClickHouse
              ↑                         |
              └─── Replay on crash ─────┘
```

This is **deferred** - Kafka remains the production choice.

---

## Clean Swap Between Kafka and Zenoh

The transport abstraction enables **zero-code-change** swaps:

```toml
# Production: Kafka
[transport]
type = "kafka"

# Dev/test: Zenoh
[transport]
type = "zenoh"
```

**What stays the same:**
- Message format (raw JSON/MsgPack)
- Orchestrator logic
- Buffer management
- ClickHouse insertion

**What changes:**
- Latency characteristics
- Durability guarantees
- Deployment topology

---

## Deferred (Not in Initial Implementation)

| Feature | Reason |
|---------|--------|
| WAL (sender-side) | Kafka has consumer groups, Zenoh acceptable for dev/test |
| Chunking | Kafka handles 1MB, Zenoh fragments internally |
| Envelope format | Not needed - raw JSON/MsgPack works |
| Compression | Transport-level (Kafka LZ4, Zenoh batching) |

---

## Performance Expectations

| Transport | Latency (p50) | Throughput | Notes |
|-----------|---------------|------------|-------|
| Kafka | 1-5ms | 100K+ msg/s | Network + broker |
| Zenoh (SHM) | 30µs | 4M+ msg/s | Same-node only |
| Zenoh (TCP) | 100-500µs | 1M+ msg/s | Cross-node |
| Memory | <1µs | 10M+ msg/s | Same-process |

---

## References

- [Zenoh 1.1.0 Release](https://zenoh.io/blog/2024-12-12-zenoh-firesong-1.1.0/)
- [Zenoh Deployment Guide](https://zenoh.io/docs/getting-started/deployment/)
- [Zenoh Shared Memory](https://zenoh-cpp.readthedocs.io/en/stable/shared_memory.html)
- [rdkafka Rust Client](https://github.com/fede1024/rust-rdkafka)

---

**Last Updated:** 2025-12-29
