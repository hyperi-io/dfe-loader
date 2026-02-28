# Project:   dfe-loader
# File:      Dockerfile
# Purpose:   Multi-arch container image (amd64 + arm64)
# Language:  Dockerfile
#
# License:   FSL-1.1-ALv2
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# Option B: COPY pre-built binary from CI artifact.
# CI cross-compiles for both architectures before the container step.

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
