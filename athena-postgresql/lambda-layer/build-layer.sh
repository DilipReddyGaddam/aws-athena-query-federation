#!/bin/bash
# Build and deploy the ADBC native PostgreSQL driver Lambda layer
#
# Prerequisites:
#   - Docker installed
#   - AWS CLI configured with appropriate permissions
#
# Usage:
#   ./build-layer.sh                    # Build only
#   ./build-layer.sh --deploy <region>  # Build and deploy to AWS

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUTPUT_DIR="${SCRIPT_DIR}/output"
LAYER_NAME="adbc-native-postgresql"
LAYER_ZIP="adbc-native-layer.zip"

echo "=== Building ADBC native PostgreSQL driver Lambda layer ==="

# Build the Docker image
docker build -t adbc-layer-builder "${SCRIPT_DIR}"

# Extract the layer zip
mkdir -p "${OUTPUT_DIR}"
docker run --rm -v "${OUTPUT_DIR}:/output" adbc-layer-builder

echo "=== Layer built: ${OUTPUT_DIR}/${LAYER_ZIP} ==="
ls -lh "${OUTPUT_DIR}/${LAYER_ZIP}"

# List contents
echo "=== Layer contents ==="
unzip -l "${OUTPUT_DIR}/${LAYER_ZIP}"

# Deploy if requested
if [[ "${1:-}" == "--deploy" ]]; then
    REGION="${2:?Usage: $0 --deploy <region>}"
    echo "=== Deploying Lambda layer to ${REGION} ==="
    
    LAYER_ARN=$(aws lambda publish-layer-version \
        --layer-name "${LAYER_NAME}" \
        --description "Native ADBC PostgreSQL driver (libadbc_driver_postgresql.so + libpq.so)" \
        --compatible-runtimes java21 \
        --compatible-architectures x86_64 \
        --zip-file "fileb://${OUTPUT_DIR}/${LAYER_ZIP}" \
        --region "${REGION}" \
        --query 'LayerVersionArn' \
        --output text)
    
    echo "=== Layer deployed: ${LAYER_ARN} ==="
    echo ""
    echo "To attach to your Lambda function:"
    echo "  aws lambda update-function-configuration \\"
    echo "    --function-name <YOUR_FUNCTION> \\"
    echo "    --layers ${LAYER_ARN} \\"
    echo "    --region ${REGION}"
    echo ""
    echo "The native libraries will be available at /opt/lib/"
    echo "Set getNativeDriverPath() to return '/opt/lib/libadbc_driver_postgresql.so'"
fi
