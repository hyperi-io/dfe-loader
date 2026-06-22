# Project:   dfe-loader
# File:      Dockerfile
# Purpose:   Production container image with dynamic librdkafka
#
# License:   BUSL-1.1
# Copyright: (c) 2026 HYPERI PTY LIMITED

FROM ubuntu:24.04

LABEL io.hyperi.profile="production"

# Runtime shared libraries for dynamically-linked Rust crates.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl netcat-openbsd iputils-ping gnupg \
    && curl -fsSL https://packages.confluent.io/clients/deb/archive.key \
       | gpg --dearmor -o /usr/share/keyrings/confluent-clients.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/confluent-clients.gpg] \
       https://packages.confluent.io/clients/deb noble main" \
       > /etc/apt/sources.list.d/confluent-clients.list \
    && apt-get update && apt-get install -y --no-install-recommends \
       librdkafka1 libssl3 zlib1g \
    && rm -rf /var/lib/apt/lists/*

COPY dfe-loader /usr/local/bin/dfe-loader
RUN chmod +x /usr/local/bin/dfe-loader

# Ubuntu 24.04 ships with ubuntu user at UID 1000 — remove before creating appuser
RUN userdel -r ubuntu && useradd --create-home --uid 1000 appuser

# GeoIP databases (DB-IP Lite, CC BY 4.0)
# Downloaded by CI via scripts/download-geoip.sh — optional, non-fatal if missing
COPY --chown=appuser:appuser geoip/ /var/lib/dfe/geoip/

USER appuser

EXPOSE 9090

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -sf http://localhost:9090/healthz > /dev/null || exit 1

ENTRYPOINT ["dfe-loader"]
CMD ["--config", "/etc/dfe/loader.yaml"]
