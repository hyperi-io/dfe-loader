<!--
  Project:      dfe-loader
  File:         docs/deployment/publishing.md
  Purpose:      Container image + Helm chart publishing via CI
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Container and chart publishing

dfe-loader ships as a multi-arch container image and a Helm chart, built and
pushed by CI on every release. dfe-infra's `versions.yaml` pins the image tag, its digest and the chart, and dfe-docker pins the image through `DFE_LOADER_VERSION`, so neither builds from source.

Both artefacts go to GHCR. The image is `ghcr.io/hyperi-io/dfe-loader` and the chart is pushed to `oci://ghcr.io/hyperi-io/charts`. CI authenticates with `GITHUB_TOKEN`, so no registry secret is needed.

One release build feeds both artefacts: the image wraps the built binary, and the chart comes from the deployment contract that binary emits.

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
    accTitle: L4 Detail -- Release artefacts
    accDescr: A release request starts a CI build of linux amd64 and arm64 binaries. The Dockerfile wraps the binaries into an image, which is pushed with its version tags. The binary's generate-artefacts emits the deployment contract, from which a thin chart is assembled and pushed. Both land in GHCR.

    subgraph release["Release"]
        direction TB
        subgraph R1["1 Build"]
            direction LR
            tag(["<b>Release request</b>"])
            build("<b>Build binaries</b>")
            tag --> build
        end
        subgraph R2["2 Package"]
            direction LR
            image("<b>Build image</b>")
            pushimage("<b>Push image tags</b>")
            contract("<b>Emit contract</b>")
            chart("<b>Assemble and<br/>push chart</b>")
            image --> pushimage
            contract --> chart
        end
        R1 --> R2
    end
    ghcr[("<b>GHCR</b>")]
    release --> ghcr
  class tag ext
  class build,image,contract,pushimage,chart part
  class ghcr store
  class tag ext
  class build,image,contract,pushimage,chart part
  class ghcr store
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
  style release fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style R1 fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style R2 fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L4 Detail -- Release artefacts. A release request is a merge with a `Release: true` trailer or a release dispatch.* Up: [Deployment](README.md).

## How it works

`.github/workflows/ci.yml` calls hyperi-ci's reusable `rust-ci.yml`. On a release it builds the `Dockerfile` for `linux/amd64` and `linux/arm64` and pushes the image with version tags. CI runs the built binary's `generate-artefacts` to emit the deployment contract as well, assembles a thin chart from it on the scalo-service library chart, and pushes that. No chart is committed to this repo. The `release:` block in `.hyperi-ci.yaml` turns both on:

```yaml
release:
  enabled: true
  helm:
    enabled: true
    contract: emit
    library: "<contract version>"
  container:
    enabled: true
    dockerfile: Dockerfile
    platforms:
      - linux/amd64
      - linux/arm64
```

`library` moves with the scalo version in `Cargo.toml`: a scalo-service release ships the schema for, and renders, only the contract version its scalo release writes. To see the chart a release would ship, build the binary and run `hyperi-ci chart assemble --binary target/debug/dfe-loader --image ghcr.io/hyperi-io/dfe-loader:<tag>@sha256:<digest> --version <version>`. It prints the chart directory it wrote.

The `Dockerfile` is generated from the app's deployment contract by scalo and copies in the binary CI built. Do not edit it by hand. Regenerate it with `dfe-loader --emit-dockerfile`. `tests/integration/helm_contract.rs` fails when the committed `Dockerfile` drifts from the contract.

For a stable release `v<version>`:

- `ghcr.io/hyperi-io/dfe-loader:v<version>`
- `ghcr.io/hyperi-io/dfe-loader:latest`
- `ghcr.io/hyperi-io/dfe-loader:sha-<commit>`

A prerelease (e.g. `v<version>-rc.1`) gets its version tag and `sha-<commit>`
only. It never moves `latest`, and nor does a stable release on an older line when a later one holds it.

After a release, check the tags:

```bash
docker pull ghcr.io/hyperi-io/dfe-loader:v<version>
docker run --rm ghcr.io/hyperi-io/dfe-loader:v<version> --help
docker manifest inspect ghcr.io/hyperi-io/dfe-loader:v<version>
```

- dfe-infra deploys the published chart through Argo CD at the tag and digest `versions.yaml` names.
- dfe-docker's compose service takes `${IMAGE_REGISTRY:-ghcr.io/hyperi-io}/dfe-loader:${DFE_LOADER_VERSION}`, which `make stack VERSION=X.Y.Z` pins, and publishes the metrics port on `${DFE_LOADER_PROMETHEUS_PORT:-9091}`.

The container's entrypoint is `dfe-loader --config /etc/dfe/loader.yaml`, so a consumer mounts the config there.
