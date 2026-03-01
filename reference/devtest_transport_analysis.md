# Transport Analysis

**Question:** What transport should we use for dev/test and production mesh deployments?

**Previous conclusion (2025-12-29):** Zenoh for dev/test, Kafka for production.

**Updated conclusion (2026-03-02):** gRPC for dev/test AND production mesh. Kafka for durable production. Zenoh removed.

---

## Summary Recommendation

| Use Case | Recommended Transport | Rationale |
|----------|----------------------|-----------|
| **Unit tests** | `MemoryTransport` (tokio channels) | Same-process, zero overhead, no setup |
| **Integration tests** | `MemoryTransport` | Sufficient, avoids external deps |
| **Local dev (multi-process)** | **gRPC** | Explicit addressing, native ACK, same as prod |
| **CI/CD pipeline** | `MemoryTransport` | Fast, deterministic, no services |
| **Staging** | gRPC or Kafka | Depends on durability needs |
| **Production (low-latency)** | **gRPC** | At-least-once with receiver WAL |
| **Production (resilient)** | **Kafka** | Broker-side durability, replay |

**Bottom line:** gRPC (via `tonic`) replaces Zenoh for all multi-process communication. It provides native ACK (response = acknowledgement), native backpressure (HTTP/2 flow control), and native K8s integration (Service discovery, health probes). Zenoh's only advantage was SHM latency (30µs vs 200µs) — irrelevant when ClickHouse insert is 1-5ms.

---

## Why gRPC Replaces Zenoh

### What Zenoh Gave Us

| Feature | Zenoh | gRPC Equivalent |
|---------|-------|-----------------|
| Dev/test multi-process | Peer mesh, no broker | `tonic` server on localhost, no broker |
| Same-node IPC | SHM ~30µs | ~200µs (negligible vs CH insert latency) |
| Cross-node dev | TCP mesh | gRPC over TCP — same |
| Pub/sub fan-out | Native | Not needed — topology is explicit |
| Request/reply | Queryable (custom) | **Native** — built into protocol |

### What Zenoh Cost Us

1. **No built-in ACK** — must build custom protocol for production use
2. **No backpressure** — pub/sub model, subscriber can't signal "slow down"
3. **Separate transport impl** — `ZenohTransport` in rustlib, `ZenohTransportAdapter` in dfe-loader
4. **Feature flag complexity** — `transport-zenoh` conditional compilation
5. **Dependency weight** — `zenoh` crate pulls significant deps (~2MB binary increase)
6. **Different mental model** — pub/sub vs request/response, team must understand both
7. **No failure propagation** — publisher doesn't know subscriber died

### What gRPC Gives Us That Zenoh Doesn't

1. **ACK is the response** — no custom protocol needed
2. **Backpressure is HTTP/2 flow control** — built into the wire protocol
3. **K8s native** — Service discovery, health probes, load balancing just work
4. **One transport for dev AND prod** — same code path, same debugging
5. **Failure detection is immediate** — connection drop = error to caller
6. **Ecosystem tooling** — grpcurl, grpc-health-probe, observability
7. **Streaming RPCs** — bidirectional streaming for batch pipeline
8. **Proven at scale** — Vector.dev, K8s API server, all Google services

### The SHM Latency Argument Is Moot

| Transport | Same-node Latency | ClickHouse Insert | Ratio |
|-----------|-------------------|-------------------|-------|
| Zenoh SHM | ~30µs | 1-5ms | 0.6-3% |
| gRPC localhost | ~200µs | 1-5ms | 4-20% |
| gRPC UDS | ~50-100µs | 1-5ms | 1-10% |

The 170µs difference is noise in the pipeline. Not worth maintaining an entire transport.

---

## Options Analyzed

### 1. Tokio Channels (MemoryTransport)

**Best for:** Same-process unit/integration tests

```rust
// Already implemented in rustlib
let (tx, rx) = tokio::sync::mpsc::channel(1000);
```

| Aspect | Assessment |
|--------|------------|
| Latency | <1µs |
| Setup | None |
| Cross-process | No |
| Cross-node | No |
| Durability | None |
| ACK | In-process (commit tracks seq) |
| Complexity | Minimal |

**Verdict:** Perfect for unit tests. Already implemented as `MemoryTransport`.

---

### 2. gRPC (tonic)

**Best for:** Multi-process dev/test AND production mesh

```rust
// tonic server
#[tonic::async_trait]
impl DfeTransport for TransportServer {
    async fn push_events(
        &self,
        request: Request<PushEventsRequest>,
    ) -> Result<Response<PushEventsResponse>, Status> {
        // Process batch, return ACK/error
    }
}
```

| Aspect | Assessment |
|--------|------------|
| Latency | 200-500µs (TCP), 50-100µs (UDS) |
| Setup | Server address (explicit, deterministic) |
| Cross-process | Yes |
| Cross-node | Yes |
| Durability | Sender-side WAL (receiver buffer) |
| ACK | **Native** — response = acknowledgement |
| Backpressure | **Native** — HTTP/2 flow control |
| K8s integration | **Native** — Service, health probes |
| Complexity | Low (tonic is mature, well-documented) |

**Pros:**

- ACK built into protocol — response IS the acknowledgement
- Backpressure via HTTP/2 flow control — no custom protocol
- K8s Service discovery — DNS-based, automatic endpoint updates
- Proven pattern — Vector.dev uses this exact approach (agent → aggregator)
- `tonic` is the standard Rust gRPC framework (tokio ecosystem)
- Bidirectional streaming for efficient batch transfer
- TLS/mTLS built in for production security
- grpc-health-probe for K8s liveness/readiness probes

**Cons:**

- Protobuf schema definition + codegen step (one-time)
- No SHM optimisation (200µs vs 30µs — irrelevant)
- Point-to-point (no pub/sub fan-out — we don't need it)

**Verdict:** Best option for all multi-process communication. Covers dev/test AND production mesh with the same code path. Native ACK eliminates the need for custom protocols.

---

### 3. Kafka (rdkafka)

**Best for:** Production with broker-side durability

| Aspect | Assessment |
|--------|------------|
| Latency | 1-5ms |
| Setup | Broker cluster required |
| Cross-process | Yes |
| Cross-node | Yes |
| Durability | **Broker-side** — at-least-once, replay |
| ACK | Offset commit |
| Backpressure | Consumer group rebalancing |
| K8s integration | StatefulSet or managed service |
| Complexity | Medium (broker ops, consumer groups) |

**Verdict:** Production standard when durability and replay are required. KEDA autoscaling on consumer group lag.

---

### 4. Zenoh (REMOVED)

**Previously recommended for dev/test. Removed in favour of gRPC.**

Reasons for removal:

1. **Redundant** — gRPC covers all Zenoh use cases with better semantics
2. **No ACK** — would need custom protocol, essentially reimplenting gRPC
3. **No backpressure** — pub/sub model doesn't support it natively
4. **Maintenance cost** — separate transport impl + feature flag + dependency
5. **Mental model split** — team doesn't need to understand pub/sub AND request/response
6. **SHM advantage irrelevant** — 170µs difference doesn't matter vs CH insert latency

---

### 5. Previously Rejected (Unchanged)

| Transport | Verdict | Reason |
|-----------|---------|--------|
| iceoryx2 | Rejected | Single-node only, fixed types, overkill |
| NATS | Rejected | If we need a broker, we have Kafka |
| ZeroMQ | Rejected | Lower-level API, no async, manual discovery |
| Flume/Crossbeam | Rejected | Same-process only, use MemoryTransport |

---

## Comparison Matrix

| Transport | Same-Process | Cross-Process | Cross-Node | Broker | Latency | ACK | Backpressure |
|-----------|-------------|---------------|------------|--------|---------|-----|-------------|
| **tokio channels** | Yes | No | No | No | <1µs | In-process | Channel capacity |
| **gRPC (tonic)** | Yes | Yes | Yes | No | 200-500µs | **Native** | **HTTP/2** |
| **Kafka** | Yes | Yes | Yes | Required | 1-5ms | Offset commit | Consumer groups |

---

## Final Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Transport Abstraction                         │
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │   Memory    │  │   gRPC      │  │   Kafka     │              │
│  │  (tokio)    │  │  (tonic)    │  │  (rdkafka)  │              │
│  │             │  │             │  │             │              │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
│        ↑               ↑                ↑                        │
│   Unit tests      Dev/Test         Production                    │
│   Integration     Staging           (durable)                    │
│                   Prod mesh                                      │
│                   (low-latency)                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Production Mesh Topology (gRPC)

```
┌─────────────────────────────────────────────────────────────────┐
│                         K8s Cluster                              │
│                                                                  │
│  ┌──────────────┐     gRPC      ┌──────────────┐                │
│  │ dfe-receiver │ ──────────►   │  dfe-loader   │ ──► ClickHouse│
│  │   (WAL)      │   PushEvents  │  (K8s Svc)   │                │
│  └──────────────┘               └──────────────┘                │
│                                                                  │
│  ┌──────────────┐     gRPC      ┌──────────────┐                │
│  │ dfe-receiver │ ──────────►   │ transformer   │                │
│  │   (WAL)      │   PushEvents  │              │                │
│  └──────────────┘               └──────┬───────┘                │
│                                        │ gRPC                    │
│                                        ▼                         │
│                                 ┌──────────────┐                │
│                                 │  dfe-loader   │ ──► ClickHouse│
│                                 │  (K8s Svc)   │                │
│                                 └──────────────┘                │
│                                                                  │
│  ACK propagation: CH INSERT OK → loader response → gRPC OK      │
│  Failure: CH error → loader error → gRPC error → receiver retry  │
└─────────────────────────────────────────────────────────────────┘
```

**At-least-once delivery chain:**

1. Receiver writes to WAL (durable)
2. Receiver calls `PushEvents` gRPC on loader
3. Loader inserts to ClickHouse
4. Loader returns gRPC response (ACK or error)
5. On ACK: receiver removes from WAL, responds 200 to upstream
6. On error: receiver retries (same or different loader via K8s Service)

---

## Implementation Plan

### Transport Abstraction (rustlib)

| Transport | Feature Flag | Use Case | Status |
|-----------|-------------|----------|--------|
| `MemoryTransport` | `transport-memory` | Unit/integration tests | Exists |
| `GrpcTransport` | `transport-grpc` | Dev/test, production mesh | **NEW** |
| `KafkaTransport` | `transport-kafka` | Production (resilient) | Exists |

### Removed

| Transport | Feature Flag | Reason |
|-----------|-------------|--------|
| ~~`ZenohTransport`~~ | ~~`transport-zenoh`~~ | Replaced by gRPC — same capabilities, better semantics |

---

## Failure Scenario Analysis

### Loader OOMs

| Transport | Detection | Recovery |
|-----------|-----------|----------|
| **Kafka** | Consumer group rebalance | Automatic — uncommitted offsets replayed |
| **gRPC** | Connection drop → `Unavailable` error | Automatic — K8s removes pod, client retries to healthy pod |

### Transform Fails (bad message)

| Transport | Detection | Recovery |
|-----------|-----------|----------|
| **Kafka** | Consumer processes and skips | DLQ routing (existing) |
| **gRPC** | Return error in response | Receiver decides: retry, DLQ, or drop |

### Network Partition

| Transport | Detection | Recovery |
|-----------|-----------|----------|
| **Kafka** | Both sides keep working (broker buffers) | Automatic — reconnect = catch up |
| **gRPC** | Connection fails, receiver buffers in WAL | Reconnect + drain WAL |

### Receiver Crashes

| Transport | Detection | Recovery |
|-----------|-----------|----------|
| **Kafka** | Consumer group rebalance | Replay from committed offset |
| **gRPC** | Upstream gets connection error | Receiver WAL replays on restart |

---

## Vector.dev Precedent

Vector.dev solved this exact problem with their v2 protocol:

- **v1** (TCP): Custom protocol, broke with K8s dynamic IPs
- **v2** (gRPC): `vector.Vector/PushEvents` RPC, HTTP/2 multiplexing, native ACK
- **Result**: Replaced Kafka in many deployments with agent → aggregator gRPC mesh

Our approach mirrors Vector's evolution: broker (Kafka) → mesh (gRPC) with receiver-side buffering.

**In-process pattern also borrowed from Vector:**
- `BatchNotifier` / `BatchReceiver` for in-process event tracking
- `EventFinalizer` attached to `EventMetadata`, travels through pipeline
- Drop impl ensures filtered events still ACK
- Fan-out: worst status wins across all sinks

---

## Sources

- [tonic — Rust gRPC framework](https://github.com/hyperium/tonic)
- [Vector v2 Protocol](https://vector.dev/docs/setup/going-to-prod/arch/)
- [gRPC Health Checking Protocol](https://github.com/grpc/grpc/blob/master/doc/health-checking.md)
- [Eclipse Zenoh GitHub](https://github.com/eclipse-zenoh/zenoh) (removed from stack)
- [Rust Channel Benchmarks](https://github.com/fereidani/rust-channel-benchmarks)

---

**Last Updated:** 2026-03-02
**Conclusion:** gRPC replaces Zenoh. Three transports: Memory (test), gRPC (mesh), Kafka (durable).
