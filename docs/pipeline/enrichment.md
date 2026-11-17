<!--
  Project:      dfe-loader
  File:         docs/pipeline/enrichment.md
  Purpose:      IP enrichment: GeoIP, reputation, risk scoring
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Enrichment

Status: each provider is off by default and turned on by its setting, `geoip.enabled`, `enrichment.reputation.enabled` or `enrichment.risk_scoring.enabled`. Reputation reads operator blocklists alone, so of its flags only `rep_is_malicious` is ever set ([#342](https://github.com/hyperi-io/dfe-loader/issues/342)).

Enrichment adds columns to a row from the first IP field it has (`enrichment.ip_fields`, default `src_ip`, `client_ip`, `ip`, `source_ip`): GeoIP columns `geo_*`, reputation columns `rep_*` and a risk score in `risk_*`. They are written as flat columns on the promoted row, never folded into `_json`, so the payload stays as it arrived and the enrichment is queryable.

The lookups key on the first IP field present, and risk scoring weighs what GeoIP and reputation found, so it adds nothing by itself.

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
flowchart LR
    accTitle: L4 Detail -- Enrichment
    accDescr: The row from column extraction is looked up by its first IP field in GeoIP and in the reputation blocklists. Risk scoring weighs both results. The row goes on to its table's buffer with the geo, rep and risk columns added.

    extract["<b>Extract columns</b>"]

    subgraph lookups["Enrich"]
        direction LR
        rep["<b>Reputation<br/>blocklists</b>"] --> risk["<b>Risk scoring</b>"]
        geo["<b>GeoIP</b>"] --> risk
    end

    buffer["<b>Buffer per table</b>"]

    extract -->|row| lookups
    lookups -->|row| buffer
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
  class extract,geo,rep,risk,buffer part
  style lookups fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L4 Detail -- Enrichment. Each provider is off until its setting turns it on.* Up: [L3 Components -- dfe-loader](../README.md).

## Providers

| Provider | Config | What it adds |
|----------|--------|--------------|
| GeoIP | `geoip` | geographic lookup (country, city, ASN) from a local database under `geoip.auto_download.data_dir` |
| Reputation | `enrichment.reputation` | `rep_is_malicious` for an address on a file `blocklist_files` names, one IP or CIDR a line. No feed is loaded for the VPN, Tor, proxy and botnet flags, so nothing derives them, and a value the record sent is kept |
| Risk scoring | `enrichment.risk_scoring` | weighted composite score from geo + reputation, with a selectable country-risk preset |

Risk scoring runs only when GeoIP or reputation returned a result for the row, so enabling it alone writes nothing.

Every value the scorer uses is config under `enrichment.risk_scoring`, with the shipped values as defaults. `config.example.yaml` lists them all.

| Key | What it sets | Default |
|-----|--------------|---------|
| `preset` | the entry in `presets` with the country lists that apply | `global` |
| `presets` | `high` and `medium` country lists by name | `global`, `us_enterprise`, `eu_enterprise`, `apac_enterprise`, `high_security` |
| `weights` | `geo`, `reputation`, `privacy`, `threat` share of the composite | 15, 25, 25, 35 |
| `scores` | what each signal adds to its component: country class, VPN, proxy, Tor, datacenter, relay, anonymiser count, botnet, malicious, spam, scanner, and the `high_abuse_score` threshold | `config.example.yaml` |
| `levels` | lowest composite score of `low`, `medium`, `high`, `critical` | 20, 40, 60, 80 |

A `presets` entry in config replaces the shipped preset of that name, or adds one, and the rest stay as shipped. Startup and a reload refuse a `preset` that `presets` does not hold, naming the valid ones, as well as a score over 100 and level bands that do not rise strictly within 1-100.

## Where the GeoIP databases come from

With `geoip.enabled` on, startup provisions the MMDB files through scalo's `geoip_download`, then opens them with `maxminddb`. An explicit `geoip.city_db_path` or `asn_db_path` is used as given. Otherwise a file in `geoip.auto_download.data_dir` (default `/var/lib/dfe/geoip`, the volume the deployment contract mounts) younger than `max_age_days` is kept, a missing or older one is downloaded, and a failed download falls back to the old file. A download larger than `max_database_bytes` (default 512 MiB), or one that does not read as an MMDB, is refused. A missing database leaves enrichment inactive and never stops the loader.

| `geoip.provider` | City | ASN | Credentials |
|------------------|------|-----|-------------|
| `db_ip_lite` (default) | yes | yes | none |
| `max_mind_geo_lite2` | yes | yes | `auto_download.maxmind_account_id`, `maxmind_license_key` |
| `ip_info_lite` | yes | no | `auto_download.ipinfo_token` |
| `ip_locate` | no | yes | none |
| `sapics` | no | yes, DB-IP's ASN data, saved as `sapics-dbip-asn.mmdb` | none |
| `custom` | the explicit paths | the explicit paths | none |

The credentials are redacted in logs and config dumps. Supply them through the secrets layer, never as literals.

Enrichment runs after extraction and before buffering, on the promoted row. The
lookups are cached, so a hot IP is resolved once and reused. Because the columns
are flat and additive, a table opts in by declaring the enrichment
columns in its schema -- an enriched value is written only where the target
table has a column for it, the same promotion rule the rest of the pipeline
follows.
