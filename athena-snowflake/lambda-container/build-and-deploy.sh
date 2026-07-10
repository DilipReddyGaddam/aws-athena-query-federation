#!/bin/bash
# Build and deploy Snowflake ADBC Lambda container image
set -euo pipefail

REGION="${REGION:-eu-north-1}"
ACCOUNT_ID="${ACCOUNT_ID:-471112813056}"
ECR_REPO="snowflake-adbc-lambda"
ECR_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${ECR_REPO}"
FUNCTION_NAME="${FUNCTION_NAME:-3ef430b0ccbf-SNOWFLAKE-ADBC}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Building Snowflake ADBC Lambda Container ==="
echo "Region: ${REGION}"
echo "Account: ${ACCOUNT_ID}"
echo "ECR: ${ECR_URI}"
echo "Lambda: ${FUNCTION_NAME}"

# Step 1: Build the federation SDK (athena-jdbc + athena-snowflake)
echo ""
echo "=== Step 1: Building federation SDK ==="
cd "${SCRIPT_DIR}/../.."
mvn install -pl athena-jdbc,athena-snowflake -am -DskipTests -Dmaven.test.skip=true

# Step 2: Strip non-essential natives from the fat JAR to reduce size
echo ""
echo "=== Step 2: Stripping non-essential natives ==="
cd athena-snowflake/target
zip -d athena-snowflake-2022.47.1.jar \
  "arrow_dataset_jni/*" "darwin/*" "linux/i386/*" "linux/ppc64le/*" \
  "adbc_driver_jni/aarch_64/*" "com/sun/jna/aix*" "com/sun/jna/darwin*" \
  "com/sun/jna/freebsd*" "com/sun/jna/openbsd*" "com/sun/jna/sunos*" \
  "com/sun/jna/win32*" "META-INF/native/*windows*" "META-INF/native/*osx*" \
  "META-INF/native/*aarch*" "minicore/*" "win/*" "aix/*" "freebsd/*" \
  "linux/amd64/*" "linux/s390x/*" "linux/arm*" "linux/riscv64/*" \
  "linux/ppc64/*" "linux/loongarch64/*" "linux/mips64/*" 2>/dev/null || true

# Step 3: Copy JAR as Lambda.zip for container build
echo ""
echo "=== Step 3: Preparing Lambda.zip ==="
cp athena-snowflake-2022.47.1.jar "${SCRIPT_DIR}/Lambda.zip"

# Step 4: Build container image
echo ""
echo "=== Step 4: Building container image ==="
cd "${SCRIPT_DIR}"
finch build --platform linux/amd64 -t ${ECR_REPO}:latest .

# Step 5: Push to ECR
echo ""
echo "=== Step 5: Pushing to ECR ==="
aws ecr get-login-password --region "${REGION}" | \
  finch login --username AWS --password-stdin "${ECR_URI}"
finch tag ${ECR_REPO}:latest "${ECR_URI}:latest"
finch push "${ECR_URI}:latest"

# Step 6: Update Lambda function
echo ""
echo "=== Step 6: Updating Lambda function ==="
aws lambda update-function-code \
  --function-name "${FUNCTION_NAME}" \
  --image-uri "${ECR_URI}:latest" \
  --region "${REGION}"

echo ""
echo "=== Done! Lambda updated with new container image ==="
echo "Wait for Lambda to finish updating before testing:"
echo "  aws lambda get-function --function-name ${FUNCTION_NAME} --region ${REGION} --query 'Configuration.LastUpdateStatus'"
