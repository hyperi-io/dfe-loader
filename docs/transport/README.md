<!--
  Project:      dfe-loader
  File:         docs/transport/README.md
  Purpose:      Index for the transport layer docs (Kafka, gRPC)
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Transport

The transports, Kafka and gRPC, are scalo's. The loader does not implement them: it takes what scalo's `Transport` receives and feeds it into its parse stage, then on through routing, extraction and the per-table insert path. Whichever transport delivers a message, the bytes converge on the same hot path: refuse anything not JSON, parse, route to `db.table`, promote schema columns, and buffer for insert. A Memory transport exists behind the `transport-memory` cargo feature for unit tests only.

Each transport has an adapter, and both adapters hand the pipeline the same `KafkaMessage`, so nothing after them knows which transport a record came on.

```mermaid
flowchart LR
    accTitle: L4 Detail -- Transport adapters
    accDescr: scalo's KafkaTransport feeds the loader's TransportAdapter and scalo's GrpcTransport feeds the GrpcTransportAdapter. Both adapters turn each record into a KafkaMessage, which goes to the JSON parse step.

    subgraph scalo["scalo transports"]
        kafka["KafkaTransport"]
        grpc["GrpcTransport"]
    end

    kafkaadapter["TransportAdapter"]
    grpcadapter["GrpcTransportAdapter"]
    message["KafkaMessage"]
    parse["Parse JSON"]

    kafka --> kafkaadapter
    grpc --> grpcadapter
    kafkaadapter --> message
    grpcadapter --> message
    message --> parse

    classDef default fill:#F8FAFC,stroke:#64748B,stroke-width:1px,color:#0F172A
    classDef ext fill:#EEF0F2,stroke:#6B7280,stroke-width:1px,stroke-dasharray:4 3,color:#1F2937
    classDef group fill:none,stroke:#64748B,stroke-width:1px,stroke-dasharray:2 4
    class kafka,grpc ext
    class scalo group
```

*Figure 1. L4 Detail -- Transport adapters. Dashed boxes are scalo's.* Up: [L3 Components -- dfe-loader](../README.md).

## In this section

- [grpc.md](grpc.md) -- the gRPC listener: how it answers a Push, the proto, its config and the deployment contract's port.
- [common-header.md](common-header.md) -- the header columns every table starts with, and what the loader writes into each.

See [../architecture.md](../architecture.md) for where the transport layer sits in the overall system.
