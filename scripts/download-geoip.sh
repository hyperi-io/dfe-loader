#!/usr/bin/env bash
# Download DB-IP Lite MMDB databases for CI bundling
#
# These are CC BY 4.0 licensed — attribution is required.
# See: https://db-ip.com/db/lite.php
#
# Usage:
#   scripts/download-geoip.sh [output_dir]
#
# Default output: .tmp/geoip/

set -euo pipefail

YEAR_MONTH=$(date +%Y-%m)
OUT_DIR="${1:-.tmp/geoip}"

mkdir -p "$OUT_DIR"

echo "==> Downloading DB-IP Lite city database (${YEAR_MONTH})..."
if curl -sSfL -o "$OUT_DIR/dbip-city-lite.mmdb.gz" \
    "https://download.db-ip.com/free/dbip-city-lite-${YEAR_MONTH}.mmdb.gz" 2>/dev/null; then
    gunzip -f "$OUT_DIR/dbip-city-lite.mmdb.gz"
    echo "    City: $(ls -lh "$OUT_DIR/dbip-city-lite.mmdb" | awk '{print $5}')"
else
    echo "    WARN: City database download failed (non-fatal)"
fi

echo "==> Downloading DB-IP Lite ASN database (${YEAR_MONTH})..."
if curl -sSfL -o "$OUT_DIR/dbip-asn-lite.mmdb.gz" \
    "https://download.db-ip.com/free/dbip-asn-lite-${YEAR_MONTH}.mmdb.gz" 2>/dev/null; then
    gunzip -f "$OUT_DIR/dbip-asn-lite.mmdb.gz"
    echo "    ASN:  $(ls -lh "$OUT_DIR/dbip-asn-lite.mmdb" | awk '{print $5}')"
else
    echo "    WARN: ASN database download failed (non-fatal)"
fi

# Write attribution file (required by CC BY 4.0)
cat > "$OUT_DIR/ATTRIBUTION.txt" << 'EOF'
GeoIP databases provided by DB-IP (https://db-ip.com)
Licensed under Creative Commons Attribution 4.0 International (CC BY 4.0)
https://creativecommons.org/licenses/by/4.0/
EOF

echo "==> Done"
