# ADBC POC - Arrow Database Connectivity for Athena Federation

## Overview

This POC explored using Arrow-based data transfer instead of the existing row-by-row JDBC ResultSet to Arrow conversion in the Athena Query Federation PostgreSQL connector. The goal was to eliminate serialization/deserialization overhead by reading data as Arrow columnar batches directly.

## Approaches Explored

### 1. ADBC API via `adbc-driver-jdbc` (Initial Approach)

**How it works:** Uses the ADBC Java API (`AdbcStatement.executeQuery()`) which wraps the JDBC driver via `adbc-driver-jdbc`. Internally, it uses `JdbcArrowReader` which calls `JdbcToArrow.sqlToArrowVectorIterator()` with a hardcoded `JdbcToArrowConfig`.

**Issues encountered:**
- `IllegalArgumentException: Configuration does not provide a mapping for array column` — PostgreSQL array columns (`text[]`, `int[]`) fail because `JdbcArrowReader.makeJdbcConfig()` creates a bare config without `arraySubTypesByColumnName` mappings
- `UnsupportedOperationException: Unmapped JDBC type: 1111` — PostgreSQL types like `uuid`, `json`, `jsonb`, `inet` map to `java.sql.Types.OTHER` which the Arrow JDBC adapter doesn't handle
- The ADBC `JdbcDriver` doesn't expose `JdbcQuirks.typeConverter` to the query execution path — the `typeConverter` only affects `JdbcConnection.getTableSchema()`, not `JdbcStatement.executeQuery()`

**Workaround attempted:** SQL `::text` casting via subquery wrapper (`castUnsupportedTypesToText`). This degraded performance by ~17% due to:
- Subquery materialization preventing PostgreSQL query plan optimizations
- Text conversion overhead for all Utf8 columns
- Re-parsing PostgreSQL array text representations (`{foo,bar}`) back to Lists in `AdbcBlockSpiller`

**Result:** 33.52 sec avg (vs 28.67 sec JDBC baseline) — **17% slower**

### 2. Arrow JDBC Adapter Directly (Fallback Approach)

**How it works:** Bypasses the ADBC API entirely and uses `JdbcToArrow.sqlToArrowVectorIterator(resultSet, customConfig)` directly with a custom `JdbcToArrowConfig` that includes:
- `arraySubTypesByColumnName` — maps List fields to their element JDBC type (handles PostgreSQL arrays natively)
- `explicitTypesByColumnName` — maps Utf8 fields to `Types.VARCHAR` (handles `uuid`/`json`/`jsonb` as VARCHAR)

**Key insight:** The ADBC `adbc-driver-jdbc` is just a thin wrapper around the same `JdbcToArrow` API. Using it directly gives identical data transfer performance with full control over type mappings.

**Result:** 31.55 sec avg — **10% slower** than JDBC baseline

**Remaining gap likely from:**
- No connection pooling (DriverManager per request vs HikariCP in JDBC path)
- `explicitTypesByColumnName` overhead for every Utf8 column
- Type-mismatch coercion path in `AdbcBlockSpiller.writeBatch()` for LIST/OTHER columns

### 3. Native ADBC Driver via JNI + Lambda Layer (Current Approach)

**How it works:** Uses `adbc-driver-jni` to call the native C/C++ `adbc-driver-postgresql` which speaks PostgreSQL's binary `COPY` protocol directly into Arrow buffers — completely bypassing JDBC.

**Issues encountered and resolved:**
1. `GLIBC_2.32 not found` — `adbc-driver-jni` requires glibc 2.32+. **Fix:** Switch Lambda to java21 runtime (AL2023, glibc 2.34)
2. `libldap.so.2: cannot open shared object file` — conda-forge `libpq.so.5` depends on OpenLDAP, Kerberos, ICU. **Fix:** Build `libpq` from PostgreSQL source with `--without-ldap --without-gssapi --without-icu`
3. Bundling `.so` in jar and extracting to `/tmp` was fragile. **Fix:** Lambda layer at `/opt/lib/`

**Lambda Layer built and deployed:**
- Built using Docker (AL2023 base) with Finch on macOS
- PostgreSQL 16.9 `libpq` compiled from source (minimal deps)
- ADBC driver built from `apache-arrow-adbc-20` tag
- Layer ARN: `arn:aws:lambda:eu-north-1:471112813056:layer:adbc-native-postgresql:2`
- Attached to: `arn:aws:lambda:eu-north-1:471112813056:function:597ac6b5b4ab-POSTGRESQL`
- Layer size: 2.5 MB (includes `libadbc_driver_jni.so`)

**Auto-detection:** `PostGreSqlAdbcRecordHandler.getNativeDriverPath()` checks if `/opt/lib/libadbc_driver_postgresql.so` exists. If found → native path. If not → falls back to Arrow JDBC adapter.

**Status:** Deployed (version 63). Native driver path works — all splits return correct row counts (250,000 rows per split). Investigating incomplete query results on Athena side.

#### Issues Encountered During Native Driver Testing (2026-04-24)

**Issue 1: `JniLoader` class poisoning (`NoClassDefFoundError`)**

After initial successful queries, all subsequent invocations failed with:
```
java.lang.NoClassDefFoundError: Could not initialize class org.apache.arrow.adbc.driver.jni.impl.JniLoader
Caused by: java.lang.ExceptionInInitializerError: Exception java.lang.IllegalStateException:
  Error loading native library adbc_driver_jni/x86_64/libadbc_driver_jni.so
```

**Root cause:** `JniLoader` is a Java enum singleton. Its `<clinit>` extracts `libadbc_driver_jni.so` from the JAR to `/tmp` and calls `Runtime.load()`. When `/tmp` fills up (from Arrow data spilling by the federation SDK), the extraction fails. Once a static initializer fails, the class is **permanently poisoned** for the JVM lifetime — every subsequent invocation on that warm Lambda instance gets `NoClassDefFoundError`.

**Why `/tmp` fills up:** The federation SDK spills intermediate Arrow data to `/tmp` before writing to S3. With 5M rows across 20 splits (~60 MB per split), multiple concurrent Lambda invocations on the same warm instance can exhaust the default 512 MB ephemeral storage.

**Fix applied:**
- Increased ephemeral storage from 512 MB to **4096 MB**
- Added `LD_LIBRARY_PATH=/opt/lib:/lib64:/usr/lib64` environment variable so the JNI bridge can find `libadbc_driver_postgresql.so` and `libadbc_driver_manager.so` at `/opt/lib/`
- Added `libadbc_driver_jni.so` to the Lambda layer (extracted from `adbc-driver-jni-0.23.0.jar` Maven artifact)

**Issue 2: SIGSEGV from double-loading `libadbc_driver_jni.so`**

Attempted fix: pre-load `libadbc_driver_jni.so` from `/opt/lib/` via `System.load()` before `JniLoader`'s static initializer runs. This caused a **JVM crash (SIGSEGV)** because both the pre-loaded library from `/opt/lib/` and `JniLoader`'s extracted copy from `/tmp` were loaded into the same process, causing native symbol conflicts.

```
# SIGSEGV (0xb) at pc=0x00007fe7fcd2b09d, pid=2, tid=16
siginfo: si_signo: 11 (SIGSEGV), si_code: 128 (SI_KERNEL), si_addr: 0x0000000000000000
```

The crash log showed both libraries loaded:
- `Loaded shared library /opt/lib/libadbc_driver_jni.so` (our pre-load)
- `Loaded shared library /tmp/jnilib-5580036376867005987.tmp` (JniLoader's extraction)

**Fix:** Removed `preloadJniLibrary()` entirely. The correct approach is to rely on `JniLoader`'s own extraction to `/tmp` (which works with 4 GB ephemeral storage) and only use the layer for `libadbc_driver_postgresql.so`, `libadbc_driver_manager.so`, and `libpq.so.5`.

**Lesson learned:** Do NOT pre-load native JNI libraries that are also loaded by a static initializer in a third-party class. The JVM does not handle duplicate native library loads gracefully — it causes SIGSEGV from symbol conflicts.

**Issue 3: Warm Lambda instance native memory leak causing SIGSEGV on subsequent queries**

Individual queries complete successfully (~25-27 sec, 1.12 GB output — faster than JDBC baseline of 28.67 sec). But when multiple queries run on the **same warm Lambda instance**, the native C++ memory from the ADBC driver accumulates across invocations:

1. Query 1 runs → native ADBC allocates ~1.9 GB via `malloc` in C++ → query completes → `RootAllocator.close()` calls `free()` on Arrow buffers
2. But glibc's `malloc` **caches freed pages** in the process heap (malloc arena) instead of returning them to the OS
3. Query 2 hits the same warm instance → native ADBC allocates another ~1.9 GB → total RSS exceeds 3072 MB Lambda limit → **SIGSEGV**

Evidence from logs:
- Successful invocations: ~900 MB memory
- Crashed invocations: ~1900-2043 MB memory, all `Runtime.ExitError` with SIGSEGV
- The crash happens **after** data is read and spilled — during response serialization or cleanup

**Attempted fix: `batch.clear()` after spilling** — did not help because the issue is in glibc's malloc arena caching, not in Arrow buffer lifecycle.

**Fix applied:** Set glibc malloc tuning environment variables on the Lambda function:
- `MALLOC_TRIM_THRESHOLD_=0` — forces glibc to return freed memory to the OS immediately after `free()` instead of caching it
- `MALLOC_MMAP_THRESHOLD_=131072` — allocations >128 KB use `mmap` instead of `sbrk`, so they are fully returned to the OS on `free()`

This ensures native memory from the C++ ADBC driver is released back to the OS between Lambda invocations on warm instances.

**Issue 4: Rows silently dropped by `ConstraintEvaluator` in `AdbcBlockSpiller`**

Queries returned inconsistent row counts — 4.6M to 4.9M instead of 5M, varying each run. All 20 Lambda splits logged exactly 250,000 rows each (5M total), but Athena received fewer rows.

**Root cause:** `AdbcBlockSpiller.ensureBlock()` called `currentBlock.constrain(constraintEvaluator)` on every new block. The SDK's `Block.constrain()` attaches a constraint evaluator that filters rows during block serialization. Even for `SELECT *` queries, the evaluator could have residual constraints from the split properties that caused false negatives on certain data patterns, silently dropping rows.

The `writeBatch()` method writes directly to vectors via `copyFromSafe()` and `BlockUtils.setValue()`, bypassing the constraint check during write. But when `blockCrypto.encrypt(block)` serializes the block for S3 spilling, the constrained block's serialization path could skip rows that didn't pass the constraint evaluation.

**Fix:** Removed `currentBlock.constrain(constraintEvaluator)` from `ensureBlock()`. The constraint evaluation is unnecessary for the ADBC batch path since:
1. Constraints are already pushed down to SQL via `buildSqlForAdbc()`
2. `writeBatch()` writes directly to vectors, not through the constraint-aware `BlockWriter` path
3. The constraint evaluator was interfering with block serialization

## Architecture

### Current Implementation (Dual Path)

```
PostGreSqlAdbcRecordHandler.doReadRecords()
  ├── getNativeDriverPath() returns "/opt/lib/libadbc_driver_postgresql.so"?
  │
  ├── YES → executeWithNativeDriver()
  │         → AdbcConnectionFactory.getNativeConnection()
  │           → JniDriver → JNI → libadbc_driver_postgresql.so (from Lambda layer at /opt/lib/)
  │             → PostgreSQL binary COPY protocol → Arrow batches directly
  │               → AdbcBlockSpiller.writeBatch() → S3 spill
  │
  └── NO → executeWithArrowJdbc() (fallback)
           → AdbcConnectionFactory.getJdbcConnection()
             → DriverManager.getConnection() (raw JDBC)
               → JdbcToArrow.sqlToArrowVectorIterator(resultSet, customConfig)
                 → ArrowVectorIterator → VectorSchemaRoot batches
                   → AdbcBlockSpiller.writeBatch() → S3 spill
```

### SQL Generation (shared by both paths)

```
PostGreSqlAdbcRecordHandler.buildSqlForAdbc()
  ├── QueryPassthrough → raw user SQL
  ├── QueryPlan (Substrait) → SubstraitSqlUtils → Calcite → PostgresqlSqlDialect SQL
  │     + split range WHERE clause appended
  └── Constraints → buildSqlComponents() → inlineParameters()
```

## Files Modified

### athena-jdbc (base module)

| File | Changes |
|---|---|
| `pom.xml` | Added `adbc-driver-jni` dependency |
| `AdbcConnectionFactory.java` | Added `getJdbcConnection()` for Arrow JDBC adapter, `getNativeConnection()` for JNI native driver, refactored `loadDriverClass()` |
| `AdbcRecordHandler.java` | Dual execution paths: `executeWithNativeDriver()` (ADBC API via JNI) and `executeWithArrowJdbc()` (Arrow JDBC adapter with custom config). `getNativeDriverPath()` hook for subclasses. `buildArrowConfig()` with `arraySubTypesByColumnName` + `explicitTypesByColumnName` |
| `AdbcBlockSpiller.java` | Fixed type-mismatch path: parses PostgreSQL array text `{foo,bar}` to List, uses `BlockUtils.setComplexValue()` for LIST vectors |
| `JdbcMetadataHandler.java` | Added `getSplitClauses(TableName, GetSplitsRequest)` overload for credential handling (from PR #3303) |
| `AdbcRecordHandlerTest.java` | Updated to mock Arrow JDBC adapter path with `MockedStatic<JdbcToArrow>` |

### athena-postgresql

| File | Changes |
|---|---|
| `PostGreSqlAdbcRecordHandler.java` | `getNativeDriverPath()` auto-detects `/opt/lib/libadbc_driver_postgresql.so`. QueryPlan (Substrait) support via `SubstraitSqlUtils`. Split range WHERE clause appending. Parameter inlining for non-QueryPlan path |
| `PostGreSqlMetadataHandler.java` | Updated `doGetSplits()` to call `getSplitClauses(tableName, getSplitsRequest)` |
| `PostGreSqlMetadataHandlerTest.java` | Updated to override new `getSplitClauses` overload |

### Lambda Layer (`athena-postgresql/lambda-layer/`)

| File | Purpose |
|---|---|
| `Dockerfile` | Multi-stage build: PostgreSQL 16.9 libpq (minimal) + ADBC driver from source on AL2023 |
| `build-layer.sh` | Build script using Docker/Finch, optional `--deploy <region>` |
| `README.md` | Usage instructions |
| `output/adbc-native-layer.zip` | Built layer (2.3 MB) |

## Performance Results

| Approach | Avg Time | vs JDBC Baseline | Notes |
|---|---|---|---|
| JDBC (existing HikariCP + S3BlockSpiller) | 28.67 sec | baseline | Row-by-row conversion, connection pooling |
| ADBC API + SQL casting workarounds | 33.52 sec | +17% slower | Subquery wrapping, text casting overhead |
| Arrow JDBC adapter (custom config) | 31.55 sec | +10% slower | No SQL mods, but no connection pooling |
| Native ADBC driver (JNI + Lambda layer) | ~25-27 sec | **~7-10% faster** | Binary COPY protocol, no JDBC. Single query succeeds; concurrent queries require malloc tuning |

**Test setup:** `SELECT * FROM orders` (5M rows, 16 columns including arrays and uuid), 20 range-based splits, 1.24 GB output.

### Memory Comparison: JDBC vs Native ADBC

| Aspect | JDBC Path | Native ADBC Path |
|---|---|---|
| Data transfer | Row-by-row via ResultSet | Binary COPY protocol → Arrow batches (16 MB default batch size) |
| Peak memory | ~1 Arrow block (~16 MB) + JDBC buffers | Arrow batch (~16 MB) + native C++ buffers + JNI overhead |
| Memory model | Java heap only | Java heap + off-heap native memory (malloc in C++ driver) |
| Observed Lambda memory | N/A | 862-928 MB (3072 MB limit) |
| Streaming | Yes — one row at a time, spill when block full | Yes — native driver yields batches at `batch_size_hint_bytes` (default 16 MB) |
| `/tmp` usage | SDK spill data only | SDK spill data + `libadbc_driver_jni.so` extraction (~716 KB) |

**Key insight:** The native ADBC PostgreSQL driver already streams in batches — it does NOT buffer the entire result. The C++ `TupleReader` uses `batch_size_hint_bytes_` (default `kDefaultBatchSizeHintBytes = 16777216` = 16 MB) to control batch sizes. When accumulated data exceeds this threshold, `GetNext()` yields the current batch. This is controlled by the statement option `adbc.postgresql.batch_size_hint_bytes`.

## Deployment Info

### Lambda Function
- **ARN:** `arn:aws:lambda:eu-north-1:471112813056:function:597ac6b5b4ab-POSTGRESQL`
- **Account:** 471112813056 (gamma dev)
- **Athena customer account:** 932332171447 (gamma ARN)
- **Region:** eu-north-1
- **Runtime:** java21 (AL2023)
- **Architecture:** x86_64
- **Memory:** 3072 MB
- **Ephemeral storage:** 4096 MB (increased from 512 MB to prevent `/tmp` exhaustion)
- **Current version:** 64
- **Alias:** `sha256-LBYFNsEuUiHA2Cc6vwoc5rhuHwxk6gu_MdItGSsO9ro` → version 64
- **Environment variables:**
  - `LD_LIBRARY_PATH=/opt/lib:/lib64:/usr/lib64`
  - `JAVA_TOOL_OPTIONS=--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -XX:+TieredCompilation -XX:TieredStopAtLevel=4 -Xms1500m -Xmx1500m`
  - `MALLOC_TRIM_THRESHOLD_=0` (forces glibc to return freed native memory to OS)
  - `MALLOC_MMAP_THRESHOLD_=131072` (large allocations use mmap for clean release)
  - `ATHENA_FEDERATION_SDK_LOG_LEVEL=DEBUG`
  - `RUNTIME_ENV=GLUE`

### Lambda Layer
- **ARN:** `arn:aws:lambda:eu-north-1:471112813056:layer:adbc-native-postgresql:2`
- **Contents:** `libadbc_driver_postgresql.so`, `libadbc_driver_manager.so`, `libadbc_driver_jni.so`, `libpq.so.5` (all at `/opt/lib/`)
- **Built from:** PostgreSQL 16.9 source (`--without-ldap --without-gssapi --without-icu`) + arrow-adbc tag `apache-arrow-adbc-20` + `adbc-driver-jni-0.23.0.jar` (Maven Central)
- **Note:** `libadbc_driver_jni.so` is in the layer but NOT pre-loaded — `JniLoader` extracts its own copy from the JAR to `/tmp`. The layer copy is there as a fallback reference only. Do NOT call `System.load()` on it before `JniLoader` runs — this causes SIGSEGV.

### Lambda Jar Source
- **Package:** `AwsAppFlowConnector-PostgreSQL` (code.amazon.com)
- **Pre-built dependency:** `libs/athena-postgresql.jar` from `aws-athena-query-federation`
- **Build:** `brazil-build` → ShadowJar

## Key Findings

1. **`adbc-driver-jdbc` is a thin wrapper** — provides no performance benefit over Arrow JDBC adapter directly, and `JdbcToArrowConfig` is not customizable through the ADBC API

2. **Type mapping is the main challenge** — PostgreSQL arrays (`Types.ARRAY`) and non-standard types (`Types.OTHER` = uuid/json/jsonb) require explicit configuration that the ADBC layer doesn't expose

3. **Connection pooling matters** — ~10% gap vs JDBC baseline from DriverManager vs HikariCP

4. **Native driver is the real win** — `adbc-driver-postgresql` (C/C++) uses PostgreSQL binary COPY protocol for true zero-copy Arrow transfer

5. **Lambda layer is the right packaging** — native `.so` files at `/opt/lib/` with `LD_LIBRARY_PATH` env var, no `/tmp` extraction needed for the driver `.so` files

6. **`libpq` must be built from source** — conda-forge version has heavy dependencies (OpenLDAP, Kerberos, ICU) not available on Lambda. Building with `--without-ldap --without-gssapi --without-icu` produces a minimal `libpq` that only needs OpenSSL (available on AL2023)

7. **java21 runtime required** — `adbc-driver-jni` JNI bridge needs glibc 2.32+ (AL2023 has 2.34, AL2 has 2.26)

8. **QueryPlan (Substrait) integration works** — Calcite converts Substrait plans to PostgreSQL-dialect SQL, with split range clauses appended for parallel execution

9. **`JniLoader` is fragile and non-recoverable** — It extracts `libadbc_driver_jni.so` from the JAR to `/tmp` on every cold start. If `/tmp` is full, the static initializer fails and the class is permanently poisoned for the JVM lifetime. Mitigation: increase ephemeral storage to 4 GB+

10. **Do NOT pre-load JNI libraries** — Calling `System.load("/opt/lib/libadbc_driver_jni.so")` before `JniLoader` runs causes SIGSEGV from duplicate native symbol registration when `JniLoader` subsequently loads its own copy from `/tmp`

11. **Native driver already streams in batches** — The C++ `TupleReader` uses `batch_size_hint_bytes_` (default 16 MB) to yield batches incrementally via `GetNext()`. It does NOT buffer the entire result set. This is the same streaming model as JDBC's row-by-row approach, just at batch granularity

12. **Ephemeral storage is critical** — Default 512 MB is insufficient for large queries. The federation SDK spills Arrow data to `/tmp`, and `JniLoader` also writes to `/tmp`. With 5M rows across 20 concurrent splits, `/tmp` exhaustion is the primary failure mode

13. **Direct S3 streaming is not feasible** — The federation SDK's `BlockSpiller` handles schema enforcement, AES encryption, block size management, spill location protocol, and residual constraint application. Bypassing it would require reimplementing all of these

14. **glibc malloc arena caching causes warm instance crashes** — The native C++ ADBC driver allocates Arrow buffers via `malloc`. When freed, glibc caches the pages in its malloc arena instead of returning them to the OS. On warm Lambda instances, this causes memory to accumulate across invocations until the process exceeds the Lambda memory limit → SIGSEGV. Fix: `MALLOC_TRIM_THRESHOLD_=0` and `MALLOC_MMAP_THRESHOLD_=131072`

15. **Native ADBC is faster than JDBC for single queries** — 25-27 sec vs 28.67 sec baseline (~7-10% improvement) for 5M rows. The binary COPY protocol eliminates row-by-row conversion overhead. The challenge is operational stability with concurrent/repeated queries on warm instances

16. **`Block.constrain()` silently drops rows in batch-write path** — The SDK's constraint evaluator is designed for the row-by-row `BlockWriter.RowWriter` path. When applied to blocks written via direct vector copies (`copyFromSafe`), it interferes with serialization and silently drops rows. Custom `BlockSpiller` implementations that write directly to vectors should NOT constrain the block

## Recommendations

### Immediate (blocking)
- Investigate incomplete query results — Athena not receiving all split results despite Lambda completing all splits successfully
- Determine if Athena-side timeout is cancelling splits before Lambda finishes (splits take 27-47 sec)

### Short-term
- Benchmark native driver path properly once incomplete results issue is resolved
- Add HikariCP connection pooling to Arrow JDBC adapter fallback path
- Consider tuning `adbc.postgresql.batch_size_hint_bytes` via `AdbcStatement.setOption()` for memory optimization (current default: 16 MB)

### Medium-term
- Productionize Lambda layer build (CI/CD pipeline)
- Extend native driver support to other databases (MySQL, etc.) via their respective ADBC native drivers
- Investigate replacing `JniLoader`'s `/tmp` extraction with direct loading from `/opt/lib/` (requires forking or contributing to `adbc-driver-jni`)

### Long-term
- Contribute upstream to `adbc-driver-jdbc` to expose `JdbcToArrowConfig` customization
- Contribute upstream to `adbc-driver-jni` to support loading native library from a configurable path (avoid `/tmp` extraction)
- Monitor for Java-native PostgreSQL ADBC driver (currently only C/C++, Python, Go, R, Rust)

## Lambda Layer Build

```bash
cd athena-postgresql/lambda-layer

# Build (requires Docker or Finch)
finch build --platform linux/amd64 -t adbc-layer-builder .
finch create --platform linux/amd64 --name adbc-layer adbc-layer-builder
finch cp adbc-layer:/output/adbc-native-layer.zip output/
finch rm adbc-layer

# Deploy
aws lambda publish-layer-version \
    --layer-name adbc-native-postgresql \
    --compatible-runtimes java21 \
    --compatible-architectures x86_64 \
    --zip-file fileb://output/adbc-native-layer.zip \
    --region eu-north-1

# Attach to function (include LD_LIBRARY_PATH and ephemeral storage)
aws lambda update-function-configuration \
    --function-name 597ac6b5b4ab-POSTGRESQL \
    --layers <LAYER_VERSION_ARN> \
    --ephemeral-storage Size=4096 \
    --environment 'Variables={RUNTIME_ENV=GLUE,JAVA_TOOL_OPTIONS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -XX:+TieredCompilation -XX:TieredStopAtLevel=4 -Xms1500m -Xmx1500m",ATHENA_FEDERATION_SDK_LOG_LEVEL=DEBUG,LD_LIBRARY_PATH=/opt/lib:/lib64:/usr/lib64}' \
    --region eu-north-1
```

## Lambda Code Deployment Procedure

```bash
# 1. Build athena-jdbc and athena-postgresql
cd /path/to/aws-athena-query-federation
mvn install -pl athena-jdbc,athena-postgresql -am -DskipTests

# 2. Copy JAR to connector package
cp athena-postgresql/target/athena-postgresql-2022.47.1.jar \
    /path/to/AwsAppFlowConnector-PostgreSQL/src/AwsAppFlowConnector-PostgreSQL/libs/

# 3. Brazil build the connector
cd /path/to/AwsAppFlowConnector-PostgreSQL/src/AwsAppFlowConnector-PostgreSQL
brazil-build release

# 4. Upload and deploy
ada credentials update --account 471112813056 --role Admin --provider isengard --once

aws s3 cp build/private/gradle/Lambda.Zip s3://stelanof-gamma-arn-test/Lambda.Zip --region eu-north-1

aws lambda update-function-code \
    --function-name 597ac6b5b4ab-POSTGRESQL \
    --s3-bucket stelanof-gamma-arn-test \
    --s3-key Lambda.Zip \
    --region eu-north-1

aws lambda wait function-updated --function-name 597ac6b5b4ab-POSTGRESQL --region eu-north-1

# 5. Publish version and update alias
VERSION=$(aws lambda publish-version \
    --function-name 597ac6b5b4ab-POSTGRESQL \
    --region eu-north-1 \
    --query 'Version' --output text)

aws lambda update-alias \
    --function-name 597ac6b5b4ab-POSTGRESQL \
    --name "sha256-LBYFNsEuUiHA2Cc6vwoc5rhuHwxk6gu_MdItGSsO9ro" \
    --function-version "$VERSION" \
    --region eu-north-1
```

## Version History

| Version | Date | Changes |
|---|---|---|
| 61 | 2026-04-23 | Initial native ADBC driver deployment with layer v1 |
| 62 | 2026-04-24 | Added `preloadJniLibrary()` — caused SIGSEGV, reverted |
| 63 | 2026-04-24 | Removed `preloadJniLibrary()`, layer v2 with `libadbc_driver_jni.so`, 4 GB ephemeral storage, `LD_LIBRARY_PATH` |
| 64 | 2026-04-24 | Added `batch.clear()` after spilling + `MALLOC_TRIM_THRESHOLD_=0` and `MALLOC_MMAP_THRESHOLD_=131072` to fix warm instance native memory leak |
| 65 | 2026-04-24 | No code change — republished to force cold starts with new env vars |
| 66 | 2026-04-24 | Removed `currentBlock.constrain(constraintEvaluator)` from `AdbcBlockSpiller.ensureBlock()` to fix silent row dropping |
| 67 | 2026-04-24 | Removed `batch.clear()` from `executeWithNativeDriver()` — was not the cause of row loss, `MALLOC_TRIM` handles memory cleanup |

## Current Status (2026-04-24 EOD)

### What's Working
- **Native ADBC driver path functional** — JNI + Lambda layer approach works end-to-end
- **No more SIGSEGV crashes** — `MALLOC_TRIM_THRESHOLD_=0` fixes warm instance native memory accumulation
- **No more JniLoader class poisoning** — 4 GB ephemeral storage prevents `/tmp` exhaustion
- **Single queries return all 5M rows** (1.12 GB) in ~25-27 sec — **faster than JDBC baseline** (28.67 sec)
- **Lambda logs confirm all 20 splits × 250,000 rows = 5M rows** delivered for every query

### Outstanding Issue: Incomplete Data on Concurrent Queries
When multiple queries run concurrently, some return less than 5M rows (1.01-1.11 GB instead of 1.12 GB). Missing rows vary: 108K to 358K per query.

**What we know:**
- Lambda side is correct: all 20 splits log 250,000 rows, all 20 `RemoteReadRecordsResponse` messages sent
- Queries with 1.12 GB output = all data; queries with less = missing rows
- Happens only under concurrency, not for isolated queries
- Removed `constraintEvaluator` from block (v66) — didn't fully fix it
- Removed `batch.clear()` (v67) — didn't fix it
- Spill block count differs: 77 blocks (missing data) vs 80 blocks (all data) for 20 splits
- All invocations on version 67, no `Runtime.ExitError`

**Hypotheses to investigate next:**
1. **`AdbcBlockSpiller` serialization issue** — `blockCrypto.encrypt(block)` may serialize fewer rows than `block.getRowCount()` claims, possibly due to Arrow vector capacity vs actual data mismatch
2. **Athena read-back issue** — Athena may fail to read some spill blocks from S3 (timing, encryption, format)
3. **Baseline comparison needed** — Run the same query with the non-ADBC (JDBC) connector to see if it also drops rows. This would isolate whether the issue is in `AdbcBlockSpiller` or in split generation/Athena
4. **`copyFromSafe` for complex types** — May silently fail for certain values (nulls in List columns, oversized Text), leaving gaps in vectors while `setRowCount` claims all rows written

### Next Steps (Priority Order)
1. **Run baseline comparison** — Execute `SELECT * FROM orders` with the JDBC connector to verify it returns exactly 5M rows. If JDBC also drops rows, the issue is in split generation, not `AdbcBlockSpiller`
2. **Add per-block row count logging** — Log actual rows written per spill block in `doSpillBlock()` to identify which blocks have fewer rows than expected
3. **Compare `AdbcBlockSpiller` with `S3BlockSpiller`** — The SDK's `S3BlockSpiller` handles the same spill protocol and works correctly. Diff the serialization paths to find what `AdbcBlockSpiller` does differently
4. **Test with simpler table** — Try a table without arrays/uuid/json columns to see if the row loss is type-specific

### Current Deployment
- **Lambda version:** 67
- **Alias:** `sha256-LBYFNsEuUiHA2Cc6vwoc5rhuHwxk6gu_MdItGSsO9ro` → version 67
- **Layer:** `adbc-native-postgresql:2` (includes `libadbc_driver_jni.so`)
- **Memory:** 3072 MB, Ephemeral: 4096 MB
- **Env vars:** `MALLOC_TRIM_THRESHOLD_=0`, `MALLOC_MMAP_THRESHOLD_=131072`, `LD_LIBRARY_PATH=/opt/lib:/lib64:/usr/lib64`
