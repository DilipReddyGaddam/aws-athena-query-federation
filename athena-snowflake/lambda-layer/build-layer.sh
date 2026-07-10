#!/bin/bash
# Build Lambda layer with ADBC native drivers
# NOTE: This approach was abandoned in favor of container image deployment
# because zip + layer exceeds 262MB Lambda limit.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Building ADBC native driver Lambda layer ==="
echo "NOTE: Container image deployment is preferred (see lambda-container/)"

cd "${SCRIPT_DIR}"
finch build --platform linux/amd64 -t adbc-layer-builder:latest .
finch create --name adbc-layer-tmp adbc-layer-builder:latest
finch cp adbc-layer-tmp:/layer.zip ./adbc-layer.zip
finch rm adbc-layer-tmp

echo "Layer built: ${SCRIPT_DIR}/adbc-layer.zip"
echo "Publish with:"
echo "  aws lambda publish-layer-version --layer-name adbc-snowflake-driver --zip-file fileb://adbc-layer.zip --compatible-runtimes java17 java21 --region eu-north-1"
