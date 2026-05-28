<!--
  Project:      dfe-loader
  File:         docs/CONTAINER-PUBLISHING.md
  Purpose:      Spec for container image + Helm chart publishing via CI
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Container Image Publishing — dfe-loader

## Goal

Publish multi-arch container images and Helm charts for dfe-loader on every
release, so downstream projects (dfe-docker, dfe-operator) can reference an
OCI tag instead of building from source or downloading binaries.

Default registry is JFrog (internal: `hyperi-docker-local`,
`hyperi-helm-local`). GHCR remains available as an opt-in alternative for
public images via `registry: ghcr` in `.hyperi-ci.yaml`.

## How It Works

The CI submodule has built-in container + Helm publishing. On release, it:

1. Builds the Dockerfile for `linux/amd64` and `linux/arm64`
2. Pushes to the configured registry with semantic version tags
3. Packages and pushes the Helm chart as an OCI artifact
4. Verifies both artefacts

JFrog publishing uses a scoped token in `ARTIFACTORY_TOKEN`. GHCR uses
`GITHUB_TOKEN` automatically.

## Tags Generated

For a release `v1.6.13` on JFrog:

- `hypersec.jfrog.io/hyperi-docker-local/dfe-loader:1.6.13`
- `hypersec.jfrog.io/hyperi-docker-local/dfe-loader:1.6`
- `hypersec.jfrog.io/hyperi-docker-local/dfe-loader:1`
- `hypersec.jfrog.io/hyperi-docker-local/dfe-loader:latest`
- `hypersec.jfrog.io/hyperi-docker-local/dfe-loader:sha-<commit>`

GHCR emits equivalent tags under `ghcr.io/hyperi-io/dfe-loader`.

Pre-release versions (e.g. `1.6.13-beta.1`) only get the full version tag.

## Implementation

### 1. Create Dockerfile

Create `Dockerfile` in the repo root. The binary is already cross-compiled in
CI for both amd64 and arm64, so the Dockerfile wraps the pre-built binary
rather than compiling from source.

**Option A — Download from JFrog at build time:**

```dockerfile
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl netcat-openbsd iputils-ping \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
ARG VERSION

# Map Docker platform arch to binary suffix
RUN case "${TARGETARCH}" in \
      amd64) SUFFIX="linux-amd64" ;; \
      arm64) SUFFIX="linux-arm64" ;; \
      *) echo "Unsupported: ${TARGETARCH}" && exit 1 ;; \
    esac && \
    curl -sf -o /usr/local/bin/dfe-loader \
        "https://github.com/hyperi-io/dfe-loader/releases/download/v${VERSION}/dfe-loader-v${VERSION}-${SUFFIX}" && \
    chmod +x /usr/local/bin/dfe-loader

RUN useradd --create-home --uid 1000 appuser
USER appuser

EXPOSE 9090

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -sf http://localhost:9090/healthz > /dev/null || exit 1

ENTRYPOINT ["dfe-loader"]
CMD ["--config", "/etc/dfe/loader.yaml"]
```

**Option B — COPY from CI build artifact (recommended):**

```dockerfile
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl netcat-openbsd iputils-ping \
    && rm -rf /var/lib/apt/lists/*

COPY dfe-loader /usr/local/bin/dfe-loader
RUN chmod +x /usr/local/bin/dfe-loader

RUN useradd --create-home --uid 1000 appuser
USER appuser

EXPOSE 9090

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -sf http://localhost:9090/healthz > /dev/null || exit 1

ENTRYPOINT ["dfe-loader"]
CMD ["--config", "/etc/dfe/loader.yaml"]
```

**Recommended:** Option B (COPY from CI artifact). The CI publish workflow
already builds the binary before the container step runs — no circular
dependency.

### 2. Update `.hyperi-ci.yaml`

Add the container publishing section:

```yaml
publish:
  enabled: true
  target: internal
  binaries: both

  container:
    enabled: true
    registry: jfrog          # or ghcr for public images
    dockerfile: Dockerfile
    platforms:
      - linux/amd64
      - linux/arm64

  helm:
    enabled: true
    registry: jfrog
```

JFrog publishes to `hyperi-docker-local` (containers) and `hyperi-helm-local`
(charts). Both repos must be `packageType: docker` with `enableDockerSupport: true`
for OCI push — helm-type repos do not support OCI.

### 3. Update CI submodule

```bash
git submodule update --remote ci
```

### 4. Update publish workflow

Regenerate or update `.github/workflows/publish.yml` to include container
publishing inputs. The CI actions/jobs/publish composite action already handles
container publishing when the config is detected.

## Verification

After a release:

```bash
# Pull and verify
docker pull ghcr.io/hyperi-io/dfe-loader:1.6.13
docker run --rm ghcr.io/hyperi-io/dfe-loader:1.6.13 --help

# Check multi-arch
docker manifest inspect ghcr.io/hyperi-io/dfe-loader:1.6.13
```

## Consumer Usage

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
    image: ghcr.io/hyperi-io/dfe-loader:1.6.13
    ports:
      - containerPort: 9090
```

## Container Checklist

Per HyperI Docker standards:

- [ ] Non-root user (UID 1000)
- [ ] Debug utilities included (curl, netcat)
- [ ] HEALTHCHECK defined
- [ ] EXPOSE ports declared
- [ ] Exec form ENTRYPOINT (receives SIGTERM)
- [ ] No secrets baked in
- [ ] Multi-arch (amd64 + arm64)
