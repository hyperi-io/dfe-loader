<!--
  Project:      dfe-loader
  File:         docs/deployment/PUBLISHING.md
  Purpose:      Container image + Helm chart publishing via CI
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Container and chart publishing

dfe-loader ships as a multi-arch container image and a Helm chart, built and
pushed by CI on every release. Downstream projects (dfe-docker, dfe-operator)
reference an immutable OCI tag instead of building from source or downloading
loose binaries.

Both artefacts go to GHCR. The image is `ghcr.io/hyperi-io/dfe-loader` and the chart is pushed to `oci://ghcr.io/hyperi-io/charts`. CI authenticates with `GITHUB_TOKEN`, so no registry secret is needed.

```mermaid
flowchart TB
    REL["Release tag (vX.Y.Z)"]
    BIN["CI cross-build<br/>linux/amd64 + linux/arm64 binaries"]
    DOCK["Build Dockerfile<br/>wraps pre-built binary"]
    PUSH["Push image<br/>version tags"]
    EMIT["dfe-loader generate-artefacts<br/>emits the deployment contract"]
    HELM["Assemble thin chart on scalo-service<br/>package + push OCI artifact"]
    REG[("GHCR")]

    REL --> BIN --> DOCK --> PUSH --> REG
    BIN --> EMIT --> HELM --> REG
```

## How it works

`.github/workflows/ci.yml` calls hyperi-ci's reusable `rust-ci.yml`. On a release it builds the `Dockerfile` for `linux/amd64` and `linux/arm64` and pushes the image with version tags. It also runs the built binary's `generate-artefacts` to emit the deployment contract, assembles a thin chart from it on the scalo-service library chart, and pushes that. No chart is committed to this repo. The `release:` block in `.hyperi-ci.yaml` turns both on:

```yaml
release:
  enabled: true
  helm:
    enabled: true
    contract: emit
    library: "2.14.3"
  container:
    enabled: true
    dockerfile: Dockerfile
    platforms:
      - linux/amd64
      - linux/arm64
```

`library` moves with the scalo version in `Cargo.toml`: a scalo-service release ships the schema for, and renders, only the contract version its scalo release writes. To see the chart a release would ship, build the binary and run `hyperi-ci chart assemble --binary target/debug/dfe-loader --image ghcr.io/hyperi-io/dfe-loader:<tag>@sha256:<digest> --version <version>`. It prints the chart directory it wrote.

The `Dockerfile` is generated from the app's deployment contract by scalo and copies in the binary CI has already cross-compiled. Do not edit it by hand. Regenerate it with `dfe-loader --emit-dockerfile`. `tests/integration/helm_contract.rs` fails when the committed `Dockerfile` drifts from the contract.

## Tags generated

For a stable release `v1.6.13`:

- `ghcr.io/hyperi-io/dfe-loader:v1.6.13`
- `ghcr.io/hyperi-io/dfe-loader:latest`
- `ghcr.io/hyperi-io/dfe-loader:sha-<commit>`

A prerelease (e.g. `v1.6.13-rc.1`) gets its version tag and `sha-<commit>`
only. It never moves `latest`.

## Verification

After a release:

```bash
docker pull ghcr.io/hyperi-io/dfe-loader:v1.6.13
docker run --rm ghcr.io/hyperi-io/dfe-loader:v1.6.13 --help
docker manifest inspect ghcr.io/hyperi-io/dfe-loader:v1.6.13
```

## Consumer usage

### Docker Compose (dfe-docker)

```yaml
services:
  dfe-loader:
    image: ghcr.io/hyperi-io/dfe-loader:${DFE_LOADER_VERSION:-latest}
    ports:
      - "9090:9090"
    volumes:
      - ./config/loader.yaml:/etc/dfe/loader.yaml:ro
```

### Kubernetes (dfe-operator)

```yaml
containers:
  - name: dfe-loader
    image: ghcr.io/hyperi-io/dfe-loader:v1.6.13
    ports:
      - containerPort: 9090
```
