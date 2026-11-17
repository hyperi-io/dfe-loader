<!--
  Project:      dfe-loader
  File:         docs/clickhouse/tls.md
  Purpose:      TLS transport security and private-CA trust for ClickHouse
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# TLS and private CAs

With TLS on the loader speaks HTTPS (`clickhouse.protocol: http`, usually 8443) or native TLS (`native`, usually 9440), and either verifies the server's certificate and host name against the container's system trust store. Native TLS trusts the system store alone, not a compiled-in root bundle, so both protocols accept the same servers.

Turning TLS on is one key, and trust comes from the system store alone, so a private CA is added there rather than set in config.

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
    accTitle: L4 Detail -- TLS trust
    accDescr: clickhouse.tls.enabled turns TLS on. The system trust store, the image bundle or the file SSL_CERT_FILE or SSL_CERT_DIR names, supplies the CA certificates. The loader verifies the server's certificate chain and host name against them before it opens HTTPS or native TLS to ClickHouse.

    subgraph tls["TLS to ClickHouse"]
        direction TB
        subgraph R1["1 Verify"]
            direction LR
            enabled("<b>clickhouse.tls.enabled</b>")
            store("<b>System trust store</b>")
            verify("<b>Verify chain<br/>and host name</b>")
            enabled -->|on| verify
            store -->|CA certs| verify
        end
        subgraph R2["2 Connect"]
            direction LR
            conn("<b>HTTPS or<br/>native TLS</b>")
            clickhouse[("<b>ClickHouse</b>")]
            conn --> clickhouse
        end
        R1 -->|verified| R2
    end
  class enabled,verify,conn part
  class store,clickhouse store
  class enabled,verify,conn part
  class store,clickhouse store
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
  style tls fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style R1 fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style R2 fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L4 Detail -- TLS trust. The store is the image bundle, or what `SSL_CERT_FILE` or `SSL_CERT_DIR` names.* Up: [ClickHouse insert layer](README.md).

| Setting | Effect |
|---------|--------|
| `clickhouse.tls.enabled` | TLS for the connection: HTTPS, or native TLS |
| `clickhouse.tls` absent | TLS turns on for a host on 8443 or 9440, or under `clickhouse.cloud` |

The SNI name and the name checked against the certificate are the host in `clickhouse.hosts`, so connect by a name (or IP) the certificate lists.

## Private / internal CAs

A private CA is trusted the way the system trusts it: add it to the image's trust store, or point `SSL_CERT_FILE` at a PEM bundle that holds it (`SSL_CERT_DIR` at a directory of them). Either variable replaces the platform store rather than adding to it, so a bundle that must trust public CAs as well has to hold them.

A trust store with no readable CA certificate fails the client at startup instead of connecting unverified.

`clickhouse.tls.ca_cert_file`, `cert_file`, `key_file` and `skip_verify` are refused by `config-check` and at startup: no client reads them. There is no skip-verify mode and no client-certificate (mTLS) auth.

A private-CA cluster:

```yaml
clickhouse:
  protocol: http
  tls:
    enabled: true
  hosts:
    - clickhouse.example.internal:8443
```

```text
SSL_CERT_FILE=/etc/dfe/ca-bundle.pem   # the private CA plus any public roots still needed
```

The CA bundle is non-secret and is distributed with the deployment. Keep the cluster password in a Secret, never in committed config: the chart fills `DFE_LOADER__CLICKHOUSE__PASSWORD` from the `clickhouse-password` key the deployment contract declares (`Config::secrets` in `src/config/loader.rs`). See [../configuration.md](../configuration.md) for the cascade.
