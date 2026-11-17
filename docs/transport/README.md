<!--
  Project:      dfe-loader
  File:         docs/transport/README.md
  Purpose:      Index for the transport layer docs (Kafka, gRPC)
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Transport

The transport is how records reach [dfe-loader](../architecture.md): scalo's Kafka or gRPC transport, adapted so the pipeline sees one message type.

| | |
|---|---|
| Repo | [hyperi-io/dfe-loader](https://github.com/hyperi-io/dfe-loader) |
| Image and chart | `ghcr.io/hyperi-io/dfe-loader`, and the thin chart released beside it |
| Config reference | `transport`, `kafka` and `grpc` in [config-schema.yaml](../config-schema.yaml) |
| Endpoints | gRPC Push on port 6000 with `transport: grpc`. `/metrics`, `/livez` and `/readyz` on 9090 |
| Metrics | `messages_received_total`, `kafka_offsets_committed_total`, `pipeline_retries_total`, in [operations.md](../operations.md#observe) |

The loader doesn't implement either transport. A Memory transport exists behind the `transport-memory` cargo feature for unit tests only.

```mermaid
---
config:
  theme: base
  look: neo
  fontFamily: Inter, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif
  flowchart:
    curve: linear
    nodeSpacing: 40
    rankSpacing: 64
    padding: 18
    wrappingWidth: 280
    htmlLabels: true
  elk:
    preset: modelOrder
  themeVariables:
    fontSize: 15px
    lineColor: "#64748B"
    primaryTextColor: "#0F172A"
    clusterBorder: "#0072B2"
    edgeLabelBackground: "#FFFFFF"
  themeCSS: >-
    p{margin:0}
    .node[data-look] rect,.node[data-look] path,.node[data-look] polygon,.node[data-look] .outer-path{filter:drop-shadow(0 1px 2px rgba(15,23,42,.10)) drop-shadow(0 6px 14px rgba(15,23,42,.10))}
    .node rect{rx:10px;ry:10px}
    .cluster rect{rx:16px;ry:16px}
    .node .label div{min-width:150px}
    .node.decision .label div{min-width:0}
    .cluster[data-look] rect{filter:none}
    .nodeLabel{line-height:1.35}
    .nodeLabel b{font-weight:600;font-size:15px;letter-spacing:-0.01em}
    .nodeLabel span{font-size:12.5px;font-weight:400;color:#334155}
    .cluster-label span.nodeLabel{display:inline-block;background:#FFFFFF;color:#0B2A3F;border:1.5px solid #0072B2;border-radius:999px;padding:2px 12px;font-size:11.5px;font-weight:700;letter-spacing:.09em;text-transform:uppercase}
    .flowchart-link{stroke-width:1.75px}
    .labelBkg{background:transparent!important}
    span.edgeLabel{display:inline-block;background:#FFFFFF!important;color:#1E293B;border:1px solid #94A3B8;border-radius:999px;padding:2px 10px;font-size:12px;font-weight:500}
    span.edgeLabel p{background:transparent!important}
    .label-icon{width:20px;height:20px;vertical-align:-4px;margin-right:6px}
    .ingest .label-icon{color:#0E8A7D}
    .xform .label-icon{color:#7B4FB0}
    .load .label-icon{color:#0072B2}
    .bus .label-icon{color:#A8517F}
    .store .label-icon{color:#00785A}
    .ui .label-icon{color:#D9480F}
    .ext .label-icon{color:#64748B}
---
flowchart TB
    accTitle: L4 Detail -- Transport adapters
    accDescr: scalo's KafkaTransport feeds the loader's TransportAdapter and scalo's GrpcTransport feeds the GrpcTransportAdapter. Both adapters turn each record into a KafkaMessage, which goes to the JSON parse step.

    subgraph scalo["scalo transports"]
        direction LR
        kafka("<b>KafkaTransport</b>")
        grpc("<b>GrpcTransport</b>")
        kafka ~~~ grpc
    end
    subgraph transport["Transport"]
        direction LR
        kafkaadapter("<b>TransportAdapter</b><br/><span>from KafkaTransport</span>")
        grpcadapter("<b>GrpcTransportAdapter</b><br/><span>from GrpcTransport</span>")
        message("<b>KafkaMessage</b>")
        kafkaadapter --> message
        grpcadapter --> message
    end
    parse("<b>Parse JSON</b><br/><span>the pipeline</span>")
    scalo -->|each to its adapter| transport
    transport --> parse
  class kafka,grpc ext
  class kafkaadapter,grpcadapter,message,parse part
  class kafka,grpc ext
  class kafkaadapter,grpcadapter,message,parse part
  classDef ext fill:#F4F6F8,stroke:#94A3B8,stroke-width:1.5px,stroke-dasharray:5 4,color:#0F172A
  classDef ingest fill:#FFFFFF,stroke:#0E8A7D,stroke-width:3px,color:#0F172A
  classDef xform fill:#FFFFFF,stroke:#7B4FB0,stroke-width:3px,color:#0F172A
  classDef load fill:#FFFFFF,stroke:#0072B2,stroke-width:3px,color:#0F172A
  classDef bus fill:#FFFFFF,stroke:#A8517F,stroke-width:3px,color:#0F172A
  classDef store fill:#FFFFFF,stroke:#00785A,stroke-width:3px,color:#0F172A
  classDef ui fill:#FFFFFF,stroke:#D9480F,stroke-width:3px,color:#0F172A
  classDef decision fill:#FFFFFF,stroke:#9A6700,stroke-width:3px,color:#0F172A
  classDef error fill:#FFFFFF,stroke:#C2410C,stroke-width:3px,color:#0F172A
  classDef control fill:#FFFFFF,stroke:#334155,stroke-width:3px,color:#0F172A
  classDef part fill:#FFFFFF,stroke:#94A3B8,stroke-width:1.5px,color:#0F172A
  style scalo fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style transport fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L4 Detail -- Transport adapters. Dashed boxes are scalo's.* Up: [L3 Components -- dfe-loader](../architecture.md).

## How it works

1. scalo's `KafkaTransport` consumes the configured topics, or its `GrpcTransport` serves Push on `grpc.listen`, port 6000 by default.
2. Each transport's adapter turns every record into the same `KafkaMessage`, with its topic, partition and offset.
3. The pipeline's parse step takes it from there, and nothing after it knows which transport a record came on.

## Interfaces

Kafka: the topics in `kafka.topics` or `kafka.topic_regex`, read as consumer group `kafka.group`. gRPC: scalo's Push on port 6000, which the deployment contract declares only when `transport: grpc`. The loader is a gRPC server only and never sends.

## Scaling and failure

A transport that can't start, such as Kafka with no broker reachable, is retried with backoff and counted in `pipeline_retries_total{stage="transport_start"}`. Until ClickHouse answers, the loader joins no consumer group and opens no gRPC listener. On gRPC a held batch is answered `UNAVAILABLE`, and the sender retries it.

## Key metrics

- `messages_received_total`: records in, on either transport.
- `kafka_offsets_committed_total`: flat on gRPC, which has no broker offsets.
- `pipeline_retries_total{stage="transport_start"}`: a transport that won't start.

## Related

- [grpc.md](grpc.md) -- the gRPC listener: how it answers a Push, the proto, its config and the deployment contract's port.
- [common-header.md](common-header.md) -- the header columns every table starts with, and what the loader writes into each.

See [../architecture.md](../architecture.md) for where the transport layer sits in the overall system.
