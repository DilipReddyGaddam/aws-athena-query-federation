# Snowflake ADBC POC - Progress Notes

## Goal
Evaluate the Apache Arrow Database Connectivity (ADBC) driver as a replacement for JDBC in the Athena Federation SDK Snowflake connector, with the aim of improving query performance for federated reads.

### Motivation
- **Eliminate row-by-row serialization**: JDBC's `ResultSet` API forces per-cell `getObject()` extraction, creating significant overhead for large result sets (millions of rows × many columns). ADBC returns data as native Arrow columnar batches, enabling bulk buffer transfers.
- **Reduce data conversion hops**: With JDBC, data flows as: Snowflake → Arrow (wire) → Java objects (ResultSet) → Arrow (Block/Spill). With ADBC, the path is: Snowflake → Arrow (wire) → Arrow (Block/Spill) — one fewer conversion.
- **Enable future zero-copy optimizations**: Native Arrow batches can potentially be written directly to S3 without intermediate copies, eliminating serialization overhead entirely.
- **Consistency across connectors**: ADBC provides a unified API for Arrow-native data access across databases (Snowflake, PostgreSQL, MySQL, etc.), reducing connector-specific code.
- **Leverage Snowflake's native Arrow protocol**: Snowflake supports returning query results in Arrow format natively; ADBC's Go driver connects directly to this protocol without JDBC intermediation.

## Approaches Explored

### Approach 1: ADBC API via JdbcDriver (same as PostgreSQL)
- Uses `AdbcStatement.executeQuery()` which wraps JDBC internally
- **Failed**: Snowflake JDBC driver doesn't support `ResultSet TYPE_SCROLL_INSENSITIVE` (type 1004)
- The ADBC `JdbcArrowReader` hardcodes scrollable ResultSet creation — can't be overridden
- Same limitation as PostgreSQL Approach 1

### Approach 2: Arrow JDBC Adapter (JdbcToArrow) — NOT YET TESTED for Snowflake
- Uses `JdbcToArrow.sqlToArrowVectorIterator()` with `TYPE_FORWARD_ONLY` ResultSet
- Should work with Snowflake since we control the Statement creation
- On PostgreSQL this approach was 5.4% slower than JDBC baseline (with HikariCP)
- Not pursued for Snowflake since Approach 3 (native Go driver) worked

### Approach 3: Native ADBC Go Driver via JNI + Container Image ✅ WORKING
- Snowflake ADBC driver is written in **Go** (not C++ like PostgreSQL)
- Go's garbage collector avoids the warm-instance memory corruption seen with PostgreSQL's C++ driver
- Deployed as **Lambda container image** (ECR) to bypass the 262 MB zip size limit
- **Successfully reading data** — 2M rows returned via native ADBC driver

## Architecture

```
Athena → Lambda (container image, java21/AL2023)
  ├── Metadata calls → SnowflakeMetadataHandler (JDBC, unchanged)
  └── ReadRecords → SnowflakeAdbcRecordHandler
        → JniDriver → /opt/lib/libadbc_driver_snowflake.so (Go via CGo)
          → Snowflake native Arrow protocol → Arrow batches
            → AdbcBlockSpiller.writeBatch() → copyFromSafe() → async S3 spill
```

## Deployment

### Container Image
- **ECR**: `471112813056.dkr.ecr.eu-north-1.amazonaws.com/snowflake-adbc-lambda:latest`
- **Base image**: `public.ecr.aws/lambda/java:21` (AL2023, glibc 2.34)
- **Native libs at `/opt/lib/`**:
  - `libadbc_driver_snowflake.so` (74 MB, Go compiled via CGo, stripped with `-ldflags="-s -w"`)
  - `libadbc_driver_manager.so` (C driver manager)
  - `libadbc_driver_jni.so` (JNI bridge, extracted from Maven)
- **Built from**: `apache-arrow-adbc-20` tag

### Lambda Function
- **ARN**: `arn:aws:lambda:eu-north-1:471112813056:function:3ef430b0ccbf-SNOWFLAKE-ADBC`
- **Package type**: Container image
- **Memory**: 5120 MB (higher memory = more vCPU = faster serialization)
- **Ephemeral storage**: 4096 MB
- **Timeout**: 900s
- **Environment variables**:
  - `LD_LIBRARY_PATH=/opt/lib:/lib64:/usr/lib64`
  - `JAVA_TOOL_OPTIONS=--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`
  - `HOME=/tmp` (for Go Snowflake driver CRL cache)
  - `TMPDIR=/tmp`
  - `SF_OCSP_RESPONSE_CACHE_DIR=/tmp`
  - `RUNTIME_ENV=GLUE`
- **Async destinations**:
  - On success: `arn:aws:sqs:eu-north-1:992382407014:asyncLambdaOnSuccessQueue-gamma-eu-north-1`
  - On failure: `arn:aws:sqs:eu-north-1:992382407014:asyncLambdaOnFailureQueue-gamma-eu-north-1`
- **Resource policy**: Allows `glue.gamma.amazonaws.com` and `GlueDNAConnectorServiceEcsTask-OnePod-gamma-eu-north-1` for both `InvokeFunction` and `InvokeAsync`

### Original Lambda (JDBC baseline)
- **ARN**: `arn:aws:lambda:eu-north-1:471112813056:function:3ef430b0ccbf-SNOWFLAKE`
- **Alias**: `sha256-hB1HPeEiXs6QZU892yH4P1iz_YFlXYcmxVwN1nXD9fQ` → v43
- **Runtime**: java17
- **VPC**: Removed (for Snowflake public endpoint access)

## Issues Encountered and Resolved

### 1. AWS SDK Version Conflict (v44-v54)
- `athena-snowflake.jar` bundles AWS SDK 2.42.39, framework uses older SDK
- `RequestOverrideConfiguration.authSchemeProvider()` method not found at runtime
- **Fix**: Used `query-federation-mainline` branch which has compatible framework version

### 2. Constructor Mismatch
- New JAR has `SnowflakeRecordHandler(dbConfig, connFactory, configOptions)` (3 args)
- Old module expected 7-arg constructor
- **Fix**: Updated `SnowflakeConnectorModule` to use 3-arg constructor

### 3. Missing Classes (AppflowCustomConnectorInternalSDK)
- `build.gradle.kts` on `mainline` excludes SDK JARs assuming external provider
- **Fix**: Used `query-federation-mainline` branch which includes them

### 4. Cross-Account Secrets Manager Access
- Lambda role couldn't access secret in customer account
- **Fix**: Cherry-picked credential handling from PR #3230

### 5. Lambda Size Limit (262 MB) — zip + layer
- Lambda.zip (~248 MB) + Go driver layer (75 MB) = 323 MB > 262 MB limit
- **Fix**: Deployed as container image (10 GB limit) instead of zip + layer

### 6. GLIBC_2.32 Not Found
- `adbc-driver-jni` requires glibc 2.32+, `java:17` base uses AL2 (glibc 2.26)
- **Fix**: Switched to `java:21` base image (AL2023, glibc 2.34)

### 7. JniLoader Class Poisoning / Go Driver CRL Cache Crash
- Go Snowflake driver crashed during `init()` trying to create CRL cache directory
- Poisoned `JniLoader` class permanently for the JVM lifetime
- **Fix**: Set `HOME=/tmp`, `TMPDIR=/tmp`, `SF_OCSP_RESPONSE_CACHE_DIR=/tmp`

### 8. Schema Not Found (SQL generation)
- Go native driver connects to default database (`DEV_DB`), not the target database
- Substrait-generated SQL didn't include database qualifier
- **Fix**: JDBC URL already contains `db=TPCDS&schema=tpcds100` as query params; Go driver picks them up. Also added database qualifier to fallback SQL path.

### 9. Partition Column in SELECT
- Split properties include a `partition` column that doesn't exist in Snowflake
- **Fix**: Filter out partition columns (from `split.getProperties().keySet()`) from SELECT

### 10. ResultSet TYPE_SCROLL_INSENSITIVE (Approach 1)
- ADBC `JdbcDriver` wrapper creates scrollable ResultSet internally
- Snowflake JDBC 3.28.0 doesn't support scrollable ResultSets
- **Fix**: Use native Go driver (Approach 3) which bypasses JDBC entirely

## SQL Generation (`buildSqlForAdbc`)

Handles three cases:
1. **QueryPassthrough** → raw user SQL from `getQueryPassthroughArguments()`
2. **Substrait QueryPlan** → Calcite converts to `SnowflakeSqlDialect` SQL + database qualifier + split WHERE clause
3. **Fallback** → `SELECT col1, col2 FROM "db"."schema"."table"` excluding partition columns

## Files Modified

### Federation SDK (`adbc-snowflake-poc` branch)
| File | Changes |
|---|---|
| `pom.xml` | Added `adbc.version=0.23.0`, `aws-sdk-v2.version=2.42.14` |
| `athena-jdbc/pom.xml` | Added ADBC dependencies |
| `athena-jdbc/.../AdbcRecordHandler.java` | Base class: native driver + JDBC adapter dual path, `getNativeDriverPath()`, `getNativeConnectionParams()` hooks, `doReadRecords()` uses `AdbcBlockSpiller`, `readWithConstraint()` uses `writeBatch()` with PERF timing metrics, fallback `writeRowByRow()` |
| `athena-jdbc/.../AdbcConnectionFactory.java` | HikariCP pooling, `getNativeConnection()` with Snowflake/PostgreSQL URI handling, extra params support |
| `athena-jdbc/.../AdbcBlockSpiller.java` | Batch vector copy spiller: `writeBatch()` with `copyFromSafe()`, batch accumulation to `maxBlockBytes`, async S3 spill via `ThreadPoolExecutor` + `ReadWriteLock` (matches `S3BlockSpiller` pattern) |
| `athena-snowflake/pom.xml` | Added `adbc-driver-jni` dependency |
| `athena-snowflake/.../SnowflakeAdbcRecordHandler.java` | Native driver path, Substrait SQL, partition column filtering, database qualifier |
| `athena-snowflake/.../SnowflakeDialect.java` | Calcite SQL dialect for Snowflake (from PR #3230) |
| `athena-snowflake/.../SnowflakeMetadataHandler.java` | Credential override fixes (from PR #3230) |
| `athena-snowflake/.../SnowflakeCredentialsProvider.java` | Cross-account credential handling (from PR #3230) |
| `athena-snowflake/.../connection/SnowflakeConnectionFactory.java` | Connection pooling fixes (from PR #3230) |
| `athena-snowflake/lambda-container/Dockerfile` | Multi-stage: builds Go driver + packages Lambda container |
| `athena-snowflake/lambda-container/build-and-deploy.sh` | Build + ECR push + Lambda update script |
| `athena-snowflake/lambda-layer/Dockerfile` | Lambda layer build (unused — container image approach used instead) |
| `athena-snowflake/lambda-layer/build-layer.sh` | Layer build script |

### Connector Package (`AwsAppFlowConnector-Snowflake`, `query-federation-mainline` branch)
| File | Changes |
|---|---|
| `libs/athena-snowflake-2022.47.1.jar` | Our build with ADBC + PR #3230 fixes, non-Linux natives stripped |
| `SnowflakeConnectorModule.java` | `provideSnowflakeRecordHandlerMap()` → `SnowflakeAdbcRecordHandler`; AppFlow path uses 3-arg constructor |

## Build & Deploy Procedure

```bash
# 1. Build federation SDK (athena-jdbc + athena-snowflake)
cd /Users/gaddamdg/workspace/aws-athena-query-federation
mvn install -pl athena-jdbc,athena-snowflake -am -DskipTests -Dmaven.test.skip=true

# 2. Strip non-essential natives from JAR
cd athena-snowflake/target
zip -d athena-snowflake-2022.47.1.jar \
  "arrow_dataset_jni/*" "darwin/*" "linux/i386/*" "linux/ppc64le/*" \
  "adbc_driver_jni/aarch_64/*" "com/sun/jna/aix*" "com/sun/jna/darwin*" \
  "com/sun/jna/freebsd*" "com/sun/jna/openbsd*" "com/sun/jna/sunos*" \
  "com/sun/jna/win32*" "META-INF/native/*windows*" "META-INF/native/*osx*" \
  "META-INF/native/*aarch*" "minicore/*" "win/*" "aix/*" "freebsd/*" \
  "linux/amd64/*" "linux/s390x/*" "linux/arm*" "linux/riscv64/*" \
  "linux/ppc64/*" "linux/loongarch64/*" "linux/mips64/*"

# 3. Copy to connector package and build
cp athena-snowflake-2022.47.1.jar \
  /path/to/AwsAppFlowConnector-Snowflake/src/AwsAppFlowConnector-Snowflake/libs/
cd /path/to/AwsAppFlowConnector-Snowflake/src/AwsAppFlowConnector-Snowflake
brazil-build -x checkstyleMain -x checkstyleTest -x test release

# 4. Build container image
cp build/private/gradle/Lambda.Zip /path/to/athena-snowflake/lambda-container/Lambda.zip
cd /path/to/athena-snowflake/lambda-container
finch build --platform linux/amd64 -t snowflake-adbc-lambda:latest .

# 5. Push to ECR and update Lambda
ECR_URI="471112813056.dkr.ecr.eu-north-1.amazonaws.com/snowflake-adbc-lambda"
aws ecr get-login-password --region eu-north-1 | finch login --username AWS --password-stdin "$ECR_URI"
finch tag snowflake-adbc-lambda:latest "$ECR_URI:latest"
finch push "$ECR_URI:latest"
aws lambda update-function-code \
  --function-name 3ef430b0ccbf-SNOWFLAKE-ADBC \
  --image-uri "$ECR_URI:latest" \
  --region eu-north-1
```

## Key Findings

1. **Native Go ADBC Snowflake driver works on Lambda** — successfully reads Arrow batches via Snowflake's native protocol
2. **No warm-instance memory corruption** — Go's GC handles memory cleanup (unlike PostgreSQL's C++ driver)
3. **Container image deployment required** — Go driver is 74 MB, exceeds zip + layer limits
4. **java21 (AL2023) required** — `adbc-driver-jni` needs glibc 2.32+
5. **Go driver needs writable `/tmp`** — CRL cache initialization crashes without `HOME=/tmp`
6. **Snowflake JDBC doesn't support scrollable ResultSets** — ADBC JdbcDriver wrapper is incompatible
7. **Connector framework has tight SDK version coupling** — must use `query-federation-mainline` branch
8. **PR #3230 required** — credential handling, connection pooling, and split logic for query federation
9. **Substrait SQL needs database qualifier** — Go driver connects to default DB, not target DB
10. **Single split limitation** — current metadata handler creates 1 split for tables without suitable primary key

## Performance Benchmarks

### Test Environment
- **Stage**: Gamma (ADBC) vs Prod (JDBC), both eu-north-1
- **Connector Service Endpoints**:
  - Gamma: `https://eu-north-1.gamma.connector-service.glue.aws.dev`
  - Prod: `https://eu-north-1.prod.connector-service.glue.aws.dev`
- **Snowflake**: `uya47780.snowflakecomputing.com` (TPCDS SF100)
- **Query**: `SELECT * FROM tpcds100.customer` (2M rows, 19 columns, ~612MB spill)
- **Method**: 10 parallel `StartAsyncQuery` calls per environment, warm instances
- **Account**: `610409754567` (Admin) for API calls
- **Secret**: `arn:aws:secretsmanager:eu-north-1:610409754567:secret:snowflake-gtomo2`

### Optimization Journey

| Version | GAMMA (ADBC) Avg | PROD (JDBC) Avg | Ratio | Change |
|---|---|---|---|---|
| Original (`writeRowByRow` + `S3BlockSpiller`) | 82s | 41s | 2.0x | Baseline — row-by-row cell extraction |
| `writeBatch` + `copyFromSafe` (sync spill per batch) | 60s | — | — | Eliminated Object boxing (38M getObject calls) |
| `writeBatch` + accumulate batches to 16MB | 60s | 35s | 1.7x | Reduced S3 puts from ~200 to ~37 |
| `writeBatch` + single-future async spill | 58s | 40s | 1.45x | Overlap S3 upload with next batch read |
| `writeBatch` + ThreadPoolExecutor async (S3BlockSpiller pattern) | 48s | 34s | 1.41x | Non-blocking queue, no wait between submits |
| **Final (5120MB Lambda — more vCPU)** | **41s** | **39s** | **1.05x** | CPU-bound ops benefit from more vCPU |

### Final Results — `SELECT * FROM customer` (2M rows, 5120MB Lambda)

| | PROD (JDBC) | GAMMA (ADBC) |
|---|---|---|
| **Min** | 33s | 33s |
| **Max** | 60s | 47s |
| **Avg** | 39.3s | 41.2s |
| **Ratio** | — | **1.05x** |

### Filtered Query — `WHERE c_birth_year > 1980 AND c_last_name LIKE 'S%' AND ...` (26K rows)

| | PROD (JDBC) | GAMMA (ADBC) |
|---|---|---|
| **Min** | 20s | 28s |
| **Max** | 28s | 33s |
| **Avg** | 23.1s | 31.4s |
| **Ratio** | — | 1.36x |

### PERF Metrics Breakdown (warm Lambda, 2M rows)

| Phase | Time | % of Total |
|---|---|---|
| Connect (JNI → Go → Snowflake auth) | ~1.4s | 3% |
| First batch (query execution) | ~1.2s | 3% |
| Network read (all batches from Snowflake) | ~2.5s | 6% |
| Spill (copyFromSafe + S3 PutObject) | ~37s | 88% |
| **Total Lambda execution** | ~42s | 100% |

### PERF Metrics Breakdown (warm Lambda, 26K rows filtered)

| Phase | Time | % of Total |
|---|---|---|
| Connect | ~1.4s | 30% |
| First batch | ~1.4s | 30% |
| Total read | ~2.7s | 57% |
| Spill | **70ms** | 1.5% |
| **Total Lambda execution** | ~4.7s | 100% |

### Lambda Resource Usage

| Metric | Value |
|---|---|
| Cold start (container image) | ~3-4s Init Duration |
| Warm invocation (metadata only) | ~150ms |
| Max memory used (2M rows) | ~1190-1243 MB |
| Max memory used (26K rows) | ~366 MB |
| S3 spill files (2M rows) | 37 files × ~16MB = 612MB |

## Performance Optimizations Applied

### 1. writeBatch() instead of writeRowByRow() (AdbcRecordHandler.java)
- **Problem**: Original code extracted cells one-by-one via `getObject()` (38M calls for 2M×19)
- **Fix**: Use `AdbcBlockSpiller.writeBatch()` with `copyFromSafe()` — direct buffer-to-buffer transfer
- **Impact**: 82s → 60s (27% faster)

### 2. Batch accumulation (AdbcBlockSpiller.java)
- **Problem**: Spilling every Arrow batch = ~200 S3 PutObject calls
- **Fix**: Accumulate batches in a block until `maxBlockBytes` (~16MB) before spilling
- **Impact**: ~200 → ~37 S3 puts

### 3. Async S3 spill with ThreadPoolExecutor (AdbcBlockSpiller.java)
- **Problem**: Synchronous `putObject()` blocked reading next batch (1 file per 1.2s vs prod's 2 files/s)
- **Fix**: Match S3BlockSpiller pattern — `ThreadPoolExecutor` + `LinkedBlockingQueue` + `ReadWriteLock`
  - Spills submitted without blocking caller
  - `getSpillLocations()` acquires write lock to wait for all spills to complete
  - Backpressure via blocking rejected execution handler
- **Impact**: 60s → 48s (at 3072MB), essentially parity at 5120MB

### 4. Lambda memory increase (5120MB)
- **Problem**: copyFromSafe + serialization are CPU-bound; Lambda CPU scales with memory
- **Fix**: 3072MB → 5120MB (gives ~2.9 vCPU vs ~1.7 vCPU)
- **Impact**: 48s → 41s, achieving parity with prod JDBC

## Key Findings (Updated)

1. **Native Go ADBC Snowflake driver works on Lambda** — successfully reads Arrow batches via Snowflake's native protocol
2. **No warm-instance memory corruption** — Go's GC handles memory cleanup (unlike PostgreSQL's C++ driver)
3. **Container image deployment required** — Go driver is 74 MB, exceeds zip + layer limits
4. **java21 (AL2023) required** — `adbc-driver-jni` needs glibc 2.32+
5. **Go driver needs writable `/tmp`** — CRL cache initialization crashes without `HOME=/tmp`
6. **Snowflake JDBC doesn't support scrollable ResultSets** — ADBC JdbcDriver wrapper is incompatible
7. **Connector framework has tight SDK version coupling** — must use `query-federation-mainline` branch
8. **PR #3230 required** — credential handling, connection pooling, and split logic for query federation
9. **Substrait SQL needs database qualifier** — Go driver connects to default DB, not target DB
10. **Single split limitation** — current metadata handler creates 1 split for tables without suitable primary key
11. **ADBC achieves parity with JDBC for Snowflake** — both drivers receive Arrow batches from Snowflake internally; bottleneck is S3 spill I/O
12. **Snowflake JDBC driver is Arrow-native** — since v3.x, Snowflake's JDBC driver uses Apache Arrow columnar format internally for result set transport ([Snowflake blog](https://www.snowflake.com/en/blog/fetching-query-results-from-snowflake-just-got-a-lot-faster-with-apache-arrow/)), negating ADBC's theoretical advantage over row-based JDBC
13. **Lambda memory/CPU is critical** — higher memory gives proportionally more vCPU; CPU-bound serialization and S3 upload benefit significantly (5120MB recommended)
14. **S3 spill is the dominant cost** — for large result sets, 88% of Lambda time is spent on Arrow serialization + S3 PutObject, not on Snowflake data retrieval

## Environment Variables (Explained)

| Variable | Value | Required | Reason |
|---|---|---|---|
| `RUNTIME_ENV` | `GLUE` | ✅ | Connector framework Glue-managed mode |
| `JAVA_TOOL_OPTIONS` | `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED` | ✅ | Arrow memory reflective access on java21 |
| `LD_LIBRARY_PATH` | `/opt/lib:/lib64:/usr/lib64` | ✅ | JNI bridge finds native .so files |
| `HOME` | `/tmp` | ✅ | Go Snowflake driver CRL cache writes here |
| `TMPDIR` | `/tmp` | ⚠️ Defensive | Go `os.TempDir()` defaults to /tmp anyway |
| `SF_OCSP_RESPONSE_CACHE_DIR` | `/tmp` | ⚠️ Defensive | JDBC OCSP cache (for metadata calls) |

## Testing Context

### Accounts & Credentials
```bash
# Connector service account (for GetQuerySplits, StartAsyncQuery, GetAsyncQueryStatus)
ada credentials update --account=610409754567 --role=Admin --provider=isengard --once --profile connector-svc

# Customer account (for GetTable, secret management, Glue catalog)
ada credentials update --account=932332171447 --role=Dev --provider=isengard --once --profile test-adbc

# Lambda deployment account
ada credentials update --account=471112813056 --role=Admin --provider=isengard --once --profile adbc-lambda
```

### API Endpoints
- **Gamma connector service**: `https://eu-north-1.gamma.connector-service.glue.aws.dev`
- **Prod connector service**: `https://eu-north-1.prod.connector-service.glue.aws.dev`
- **Glue gamma**: `https://glue-gamma.eu-north-1.amazonaws.com`

### Connection Object (Gamma ADBC — with VPC for Lambda routing)
```json
{
  "AthenaProperties": {"GLUE_MANAGED_CONNECTION": "true", "spill_bucket": "gaddamdg-query-splits-spill-eu-north-1"},
  "AuthenticationConfiguration": {"AuthenticationType": "BASIC", "SecretArn": "arn:aws:secretsmanager:eu-north-1:610409754567:secret:snowflake-gtomo2"},
  "CompatibleComputeEnvironments": ["SPARK", "ATHENA"],
  "ConnectionProperties": {"ROLE_ARN": "arn:aws:iam::932332171447:role/ejeffrli-managed-connectors", "DATABASE": "TPCDS", "PORT": "443", "WAREHOUSE": "DEV_WH", "CATALOG_CASING_FILTER": "LOWERCASE_ONLY", "SCHEMA": "tpcds100", "GLUE_MANAGED_CONNECTION": "true", "HOST": "uya47780.snowflakecomputing.com"},
  "ConnectionSchemaVersion": 2, "ConnectionType": "SNOWFLAKE", "Name": "snowflake-adbc-connection",
  "PhysicalConnectionRequirements": {"AvailabilityZone": "eu-north-1a", "SecurityGroupIdList": ["sg-55bc5b2c"], "SubnetId": "subnet-0618856f"},
  "Status": "READY"
}
```

### Connection Object (Prod JDBC — no VPC, managed path)
```json
{
  "AthenaProperties": {"MANAGED_CONNECTION": "true", "disable_spill_encryption": "false", "spill_bucket": "gaddamdg-query-splits-spill-eu-north-1"},
  "AuthenticationConfiguration": {"AuthenticationType": "BASIC", "SecretArn": "arn:aws:secretsmanager:eu-north-1:610409754567:secret:snowflake-gtomo2"},
  "CompatibleComputeEnvironments": ["ATHENA"],
  "ConnectionProperties": {"DATABASE": "TPCDS", "PORT": "443", "WAREHOUSE": "DEV_WH", "CATALOG_CASING_FILTER": "LOWERCASE_ONLY", "SCHEMA": "tpcds100", "HOST": "uya47780.snowflakecomputing.com"},
  "ConnectionSchemaVersion": 2, "ConnectionType": "SNOWFLAKE", "Name": "athena-365wj76u",
  "Status": "READY"
}
```

### Important Testing Notes
- **VPC in gamma connection is required** — routes to the custom ADBC Lambda via the connector service
- **No VPC for prod connection** — uses the managed connector path (JDBC)
- **CatalogId matters** — use `932332171447` for gamma (routes to custom Lambda), `610409754567` for prod
- **Substrait plans are case-sensitive** — plans must use lowercase table/column names
- **Substrait plans with filter expressions** — require correct Calcite libraries; complex plans may fail with `Failed to prepare statement with Calcite`

## Conclusion

**ADBC does NOT provide a meaningful performance advantage over JDBC for Snowflake.** Both drivers achieve near-identical performance (~33-41s for 2M rows) because:

1. Snowflake's JDBC driver already uses Apache Arrow internally since v3.x
2. Both paths are bottlenecked on S3 spill I/O (88% of execution time)
3. The Go native driver reads data in ~2.5s; remaining time is serialization + upload

**Where ADBC would provide advantage:**
- Databases with row-oriented JDBC wire protocols (MySQL, Oracle, SQL Server, older PostgreSQL)
- Scenarios where Arrow batches can be passed downstream without re-serialization
- Future optimizations: zero-copy spill (write ADBC batches directly as Arrow IPC without copyFromSafe)

## Next Steps

1. **Multi-split**: Investigate why only 1 split is created (primary key check in metadata handler) — parallelism would benefit both JDBC and ADBC equally
2. **Type handling**: Test with Snowflake-specific types (VARIANT, ARRAY, OBJECT, TIMESTAMP_TZ)
3. **Zero-copy spill**: Write ADBC Arrow batches directly to S3 as Arrow IPC without intermediate copyFromSafe — could eliminate the serialization overhead entirely
4. **Test with other databases**: Run the same benchmark against PostgreSQL or MySQL where ADBC should show a clearer advantage over JDBC
5. **Production readiness**: Error handling, connection retry, timeout configuration, Substrait filter support
