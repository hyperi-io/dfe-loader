# Dev/Test Transport Analysis

**Question:** Is direct IPC better than Zenoh for dev/test? What are the alternatives?

---

## Summary Recommendation

| Use Case | Recommended Transport | Rationale |
|----------|----------------------|-----------|
| **Unit tests** | `MemoryTransport` (tokio channels) | Same-process, zero overhead, no setup |
| **Integration tests** | `MemoryTransport` | Sufficient, avoids external deps |
| **Local dev (multi-process)** | **Zenoh peer mode** | Simple setup, no broker, mesh capable |
| **CI/CD pipeline** | `MemoryTransport` | Fast, deterministic, no services |
| **Staging** | Zenoh or Kafka | Depends on durability needs |
| **Production** | **Kafka** | At-least-once with replay |

**Bottom line:** Zenoh in peer mode is the best balance for multi-process dev/test. It requires no broker, works cross-process, and has the same API we'll use for high-performance deployments. iceoryx2 is faster but single-node only and adds complexity we don't need.

---

## Options Analyzed

### 1. Tokio Channels (MemoryTransport)

**Best for:** Same-process unit/integration tests

```rust
// Already planned in transport abstraction
let (tx, rx) = tokio::sync::mpsc::channel(1000);
```

| Aspect | Assessment |
|--------|------------|
| Latency | <1µs |
| Setup | None |
| Cross-process | ❌ No |
| Cross-node | ❌ No |
| Durability | ❌ None |
| Complexity | Minimal |

**Verdict:** Perfect for unit tests. Already planned as `MemoryTransport`.

---

### 2. Zenoh (Peer Mode)

**Best for:** Multi-process dev/test without broker

```toml
[zenoh]
mode = "peer"
listen = ["tcp/127.0.0.1:7447"]
```

| Aspect | Assessment |
|--------|------------|
| Latency | 30µs (SHM), 100-500µs (TCP) |
| Setup | None (peer discovery) |
| Cross-process | ✅ Yes (SHM or TCP) |
| Cross-node | ✅ Yes (TCP mesh) |
| Durability | ⚠️ In-flight only |
| Complexity | Low (Rust-native, single binary) |

**Pros:**

- No broker required in peer mode
- Same API for dev and production
- Rust-native (Eclipse Foundation, well-maintained)
- SHM for same-node, TCP for cross-node
- Mesh topology works without central coordinator

**Cons:**

- No persistence (acceptable for dev/test)
- Adds ~2MB to binary size

**Verdict:** Best option for multi-process dev/test. Simple setup, good performance, same abstraction as production.

---

### 3. iceoryx2

**Best for:** Ultra-low-latency same-node IPC (automotive, robotics)

```rust
use iceoryx2::prelude::*;
let service = zero_copy::Service::new("events")
    .publish_subscribe()
    .create()?;
```

| Aspect | Assessment |
|--------|------------|
| Latency | ~1µs (true zero-copy) |
| Setup | Shared memory segment |
| Cross-process | ✅ Yes |
| Cross-node | ❌ No |
| Durability | ❌ None |
| Complexity | Medium (SHM management) |

**Pros:**

- Fastest IPC available (~1µs)
- True zero-copy (no serialization)
- Rust-native (Eclipse Foundation)
- C/C++ interop

**Cons:**

- Single-node only (no networking)
- Requires shared memory setup
- Fixed message types (no dynamic JSON)
- Overkill for dev/test

**Verdict:** Not recommended. Single-node limitation and fixed types don't fit our use case. Zenoh with SHM gives us 30µs (fast enough) with cross-node capability.

---

### 4. NATS (with JetStream)

**Best for:** Cloud-native messaging with persistence

| Aspect | Assessment |
|--------|------------|
| Latency | 1-5ms |
| Setup | NATS server required |
| Cross-process | ✅ Yes |
| Cross-node | ✅ Yes |
| Durability | ✅ JetStream persistence |
| Complexity | Medium (broker deployment) |

**Pros:**

- At-least-once with JetStream
- Well-maintained, CNCF project
- Good Rust client (async-nats)

**Cons:**

- Requires broker (like Kafka)
- Higher latency than Zenoh
- No SHM optimization

**Verdict:** Not recommended for dev/test. If we need a broker, we already have Kafka. NATS doesn't add value over Kafka in our architecture.

---

### 5. ZeroMQ (zmq.rs)

**Best for:** Traditional socket-style messaging

| Aspect | Assessment |
|--------|------------|
| Latency | 10-100µs |
| Setup | None (peer-to-peer) |
| Cross-process | ✅ Yes |
| Cross-node | ✅ Yes |
| Durability | ❌ None |
| Complexity | Medium (socket patterns) |

**Pros:**

- Mature, well-understood patterns
- Native Rust implementation (zmq.rs)
- No broker for simple patterns

**Cons:**

- Lower-level API than Zenoh
- Less modern (no native async)
- No SHM optimization
- Manual discovery

**Verdict:** Not recommended. Zenoh provides similar brokerless operation with better ergonomics and SHM support.

---

### 6. Flume / Crossbeam Channels

**Best for:** Same-process multi-producer-multi-consumer

| Aspect | Assessment |
|--------|------------|
| Latency | <1µs |
| Setup | None |
| Cross-process | ❌ No |
| Cross-node | ❌ No |

**Verdict:** Same as tokio channels - good for unit tests, not for multi-process.

---

## Comparison Matrix

| Transport | Same-Process | Cross-Process | Cross-Node | Broker | Latency | Maintenance |
|-----------|-------------|---------------|------------|--------|---------|-------------|
| **tokio channels** | ✅ | ❌ | ❌ | No | <1µs | Tokio team |
| **Zenoh** | ✅ | ✅ | ✅ | Optional | 30-500µs | Eclipse |
| **iceoryx2** | ✅ | ✅ | ❌ | No | ~1µs | Eclipse |
| **NATS** | ✅ | ✅ | ✅ | Required | 1-5ms | CNCF |
| **ZeroMQ** | ✅ | ✅ | ✅ | No | 10-100µs | ZeroMQ org |
| **Kafka** | ✅ | ✅ | ✅ | Required | 1-5ms | Confluent |

---

## Final Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Transport Abstraction                        │
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │   Memory    │  │   Zenoh     │  │   Kafka     │             │
│  │  (tokio)    │  │  (peer/     │  │  (rdkafka)  │             │
│  │             │  │   client)   │  │             │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
│        ↑               ↑                ↑                       │
│   Unit tests      Dev/Test         Production                   │
│   Integration     Staging*          (at-least-once)             │
│                   Low-latency                                   │
└─────────────────────────────────────────────────────────────────┘

* Staging with Zenoh acceptable if data loss is tolerable
```

---

## Why NOT iceoryx2?

Despite being the fastest option, iceoryx2 doesn't fit our needs:

1. **Single-node only**: We need cross-node for K8s deployments
2. **Fixed types**: We use dynamic JSON, iceoryx2 prefers fixed structs
3. **Overkill**: 30µs (Zenoh SHM) vs 1µs (iceoryx2) - both are negligible compared to ClickHouse insert latency
4. **Additional abstraction**: Would need separate transport impl that doesn't generalize to production

---

## Why Zenoh for Dev/Test?

1. **Same API as production path**: `ZenohTransport` works in dev and can be production-ready
2. **No broker**: Peer mode requires no infrastructure
3. **SHM optimization**: Same-node communication is fast (30µs)
4. **Rust-native**: No FFI, pure Rust implementation
5. **Active development**: Eclipse Foundation, v1.1.0 released Dec 2024
6. **Mesh capable**: Can scale from 2 processes to N nodes

---

## Implementation Plan

Keep the three-transport design from `transport_abstraction_spec.md`:

| Transport | Feature Flag | Use Case |
|-----------|--------------|----------|
| `MemoryTransport` | `transport-memory` | Unit/integration tests |
| `ZenohTransport` | `transport-zenoh` | Dev/test, low-latency prod |
| `KafkaTransport` | `transport-kafka` | Production (resilient) |

**No changes needed** - the current design is correct.

---

## Sources

- [Eclipse Zenoh GitHub](https://github.com/eclipse-zenoh/zenoh)
- [Eclipse iceoryx2 GitHub](https://github.com/eclipse-iceoryx/iceoryx2)
- [NATS Rust Example](https://natsbyexample.com/examples/messaging/pub-sub/rust)
- [ZeroMQ Rust (zmq.rs)](https://github.com/zeromq/zmq.rs)
- [Flume Channel](https://lib.rs/crates/flume)
- [iceoryx2 on crates.io](https://crates.io/crates/iceoryx2)
- [Rust Channel Benchmarks](https://github.com/fereidani/rust-channel-benchmarks)

---

**Last Updated:** 2025-12-29
**Conclusion:** Zenoh is the right choice for dev/test. iceoryx2 is overkill and single-node only.

