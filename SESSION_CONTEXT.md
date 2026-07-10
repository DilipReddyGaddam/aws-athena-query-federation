# Session Context — ADBC POC Recovery & AppFlow OOM Fix

## Overview

Two main tasks were completed in this session:

1. **AppFlow ConnectorProfile OOM Fix** — Fixed a thread leak causing canary failures in CloudFormation resource providers
2. **Athena Federation SDK ADBC POC Recovery** — Recovered lost code, rebuilt, and deployed the Snowflake ADBC (Arrow Database Connectivity) POC Lambda

---

## 1. AppFlow ConnectorProfile OOM Fix (CR-286166122)

### Problem
`AWS::AppFlow::ConnectorProfile` LIST handler in CFN canary account was hitting `java.lang.OutOfMemoryError: unable to create native thread` because `ClientBuilder.getClient()` created a **new AppflowClient per invocation** without ever closing them. Threads accumulated on warm Lambda containers.

### Fix
Made `AppflowClient` a static singleton in all three resource providers, using `LambdaWrapper.HTTP_CLIENT` (shared framework HTTP client) while preserving `apiCallTimeout(1 min)`.

### Package
- **Repo**: `AWSCloudFormationResourceProvidersAppFlow` (code.amazon.com)
- **Workspace**: `/Users/gaddamdg/workspace/AppFlowFix/`
- **CR**: https://code.amazon.com/reviews/CR-286166122
- **Files changed**:
  - `aws-appflow-connectorprofile/src/main/java/.../ClientBuilder.java`
  - `aws-appflow-connector/src/main/java/.../ClientBuilder.java`
  - `aws-appflow-flow/src/main/java/.../ClientBuilder.java`

---

## 2. ADBC POC — Snowflake Native Go Driver via JNI

### What is this POC?
Evaluating Apache Arrow ADBC as a replacement for JDBC in the Athena Federation SDK Snowflake connector. Uses the native Go Snowflake ADBC driver via JNI to achieve Arrow-native data transfer without row-by-row serialization.

**Key results**: 12-24% faster single queries, 40% faster under concurrent load (100 parallel queries).

### Documents
- **POC doc (recovered)**: `/Users/gaddamdg/workspace/aws-athena-query-federation/athena-snowflake/SNOWFLAKE_ADBC_POC.md`
- **Quip design doc**: https://quip-amazon.com/ZQFiAgaoVXVJ/Arrow-Database-Connectivity-ADBC-Support-via-Managed-Connectors-POC-results
- **Upstream PR with credential fixes**: https://github.com/awslabs/aws-athena-query-federation/pull/3230

### Architecture
```
Athena → Lambda (container image, java21/AL2023)
  ├── Metadata calls → SnowflakeMetadataHandler (JDBC, unchanged)
  └── ReadRecords → SnowflakeAdbcRecordHandler
        → JniDriver → /opt/lib/libadbc_driver_snowflake.so (Go via CGo)
          → Snowflake native Arrow protocol → Arrow batches
            → S3BlockSpiller → S3 spill
```

### Lambda
- **ARN**: `arn:aws:lambda:eu-north-1:471112813056:function:3ef430b0ccbf-SNOWFLAKE-ADBC`
- **ECR**: `471112813056.dkr.ecr.eu-north-1.amazonaws.com/snowflake-adbc-lambda:latest`
- **Memory**: 5120 MB
- **Runtime**: Container image (java21/AL2023)
- **Status**: ✅ Deployed and working (57-80s for catalog_sales on TPCDS100)

### Workspaces

#### Federation SDK (GitHub)
- **Location**: `/Users/gaddamdg/workspace/aws-athena-query-federation/`
- **Repo**: https://github.com/DilipReddyGaddam/aws-athena-query-federation
- **Branch**: `adbc-snowflake-poc`
- **Base**: PR #3230 (`pr-snowflake` branch from awslabs/aws-athena-query-federation)
- **Key changes on top of PR #3230**:
  - `pom.xml` — added `<adbc.version>0.23.0</adbc.version>`
  - `athena-jdbc/pom.xml` — added adbc-core, adbc-driver-manager, adbc-driver-jdbc, adbc-driver-jni deps
  - `athena-snowflake/pom.xml` — added adbc-driver-jni dep
  - `athena-jdbc/.../AdbcRecordHandler.java` — base ADBC record handler (decompiled, doReadRecords removed — uses base class)
  - `athena-jdbc/.../AdbcBlockSpiller.java` — batch vector copy spiller with async S3 spill
  - `athena-jdbc/.../AdbcConnectionFactory.java` — ADBC connection factory with JNI driver support (key: `"jni.driver"`)
  - `athena-snowflake/.../SnowflakeAdbcRecordHandler.java` — Snowflake-specific ADBC implementation
  - `athena-snowflake/.../SnowflakeDialect.java` — Calcite SQL dialect for Snowflake
  - `athena-snowflake/lambda-container/Dockerfile` — container image build
  - `athena-snowflake/lambda-container/build-and-deploy.sh` — deploy script

#### Connector Package (Brazil)
- **Location**: `/Volumes/BrazilWorkspace/glue/AwsAppFlowConnector-Snowflake/`
- **Packages**:
  - `AwsAppFlowConnector-Snowflake` — the connector Lambda package
  - `GlueDNAAthenaConnectorAdapter` — adapter between Glue DNA and Athena Federation SDK
- **Key changes in AwsAppFlowConnector-Snowflake**:
  - `src/main/java/.../dagger/SnowflakeConnectorModule.java` — `provideSnowflakeRecordHandlerMap()` returns `SnowflakeAdbcRecordHandler`
  - `libs/athena-snowflake-2022.47.1.jar` — our ADBC-enabled fat JAR (stripped of non-Linux natives)
  - `build.gradle.kts` — no SDK class exclusions (both SDK versions coexist on classpath)
- **GlueDNAAthenaConnectorAdapter**: unchanged, uses `aws-athena-federation-sdk-2025.6.1-withdep.jar`

### Build & Deploy Procedure

```bash
# 1. Build federation SDK
cd /Users/gaddamdg/workspace/aws-athena-query-federation
mvn install -pl athena-federation-sdk,athena-jdbc,athena-snowflake -am -DskipTests -Dmaven.test.skip=true -Dcheckstyle.skip=true

# 2. Strip non-essential natives from fat JAR
cd athena-snowflake/target
zip -d athena-snowflake-2022.47.1.jar \
  "arrow_dataset_jni/*" "darwin/*" "linux/i386/*" "linux/ppc64le/*" \
  "adbc_driver_jni/aarch_64/*" "com/sun/jna/aix*" "com/sun/jna/darwin*" \
  "com/sun/jna/freebsd*" "com/sun/jna/openbsd*" "com/sun/jna/sunos*" \
  "com/sun/jna/win32*" "META-INF/native/*windows*" "META-INF/native/*osx*" \
  "META-INF/native/*aarch*" "minicore/*" "win/*" "aix/*" "freebsd/*" \
  "linux/amd64/*" "linux/s390x/*" "linux/arm*" "linux/riscv64/*" \
  "linux/ppc64/*" "linux/loongarch64/*" "linux/mips64/*"

# 3. Copy to connector package
cp athena-snowflake-2022.47.1.jar /Volumes/BrazilWorkspace/glue/AwsAppFlowConnector-Snowflake/src/AwsAppFlowConnector-Snowflake/libs/

# 4. Build connector package
cd /Volumes/BrazilWorkspace/glue/AwsAppFlowConnector-Snowflake/src/AwsAppFlowConnector-Snowflake
brazil-build -x checkstyleMain -x checkstyleTest -x test release

# 5. Copy Lambda.zip and build container
LAMBDA_ZIP="/Volumes/BrazilWorkspace/glue/AwsAppFlowConnector-Snowflake/build/AwsAppFlowConnector-Snowflake/AwsAppFlowConnector-Snowflake-1.0/AL2_x86_64/DEV.STD.PTHREAD/build/private/gradle/Lambda.zip"
cp "$LAMBDA_ZIP" /Users/gaddamdg/workspace/aws-athena-query-federation/athena-snowflake/lambda-container/Lambda.zip
cd /Users/gaddamdg/workspace/aws-athena-query-federation/athena-snowflake/lambda-container
finch build --platform linux/amd64 -t snowflake-adbc-lambda:latest .

# 6. Push to ECR and update Lambda
ada credentials update --account 471112813056 --role Admin --provider isengard --once
ECR_URI="471112813056.dkr.ecr.eu-north-1.amazonaws.com/snowflake-adbc-lambda"
aws ecr get-login-password --region eu-north-1 | finch login --username AWS --password-stdin 471112813056.dkr.ecr.eu-north-1.amazonaws.com
finch tag snowflake-adbc-lambda:latest "$ECR_URI:latest"
finch push "$ECR_URI:latest"
aws lambda update-function-code --function-name "3ef430b0ccbf-SNOWFLAKE-ADBC" --image-uri "$ECR_URI:latest" --region eu-north-1
aws lambda wait function-updated --function-name "3ef430b0ccbf-SNOWFLAKE-ADBC" --region eu-north-1
```

### Issues Resolved During Deployment

| Issue | Root Cause | Fix |
|-------|-----------|-----|
| `Secrets Manager: not authorized` | Lambda role used instead of federated identity creds | Rebased on PR #3230 which has credential override fixes |
| `Decompilation failed` (SerDe) | Decompiled SDK classes (Block.java, SpillConfig.java) broke Jackson serialization | Reverted SDK modifications — use original PR #3230 SDK |
| `Decompilation failed` (doReadRecords) | CFR decompiler couldn't handle complex try-with-resources in AdbcRecordHandler.doReadRecords() | Removed the override — let base RecordHandler.doReadRecords() handle it with S3BlockSpiller |
| `[JNI] Must provide String AdbcOptionKey{jni.driver}` | Used wrong parameter key `"adbc.driver.lib.path"` | Changed to `"jni.driver"` (the actual key from `JniDriver.PARAM_DRIVER`) |
| Constructor access errors in SnowflakeConnectorModule | New JAR has different constructor visibility | Used public 3-arg constructors: `(dbConfig, connFactory, configOptions)` |

### Important Notes

1. **SDK version mismatch**: The `GlueDNAAthenaConnectorAdapter` uses SDK v2025.6.1, while `athena-snowflake` JAR bundles SDK v2022.47.1 (from PR #3230). Both coexist on classpath. Metadata operations use the adapter's SDK; ReadRecords uses the snowflake JAR's handlers.

2. **Decompiled code limitations**: The `AdbcRecordHandler.doReadRecords()` method couldn't be decompiled by CFR. We removed the override so the base class `RecordHandler.doReadRecords()` runs, which creates an `S3BlockSpiller` and calls `readWithConstraint()`. This means we're NOT using the `AdbcBlockSpiller` (batch copy) path — we're using the `writeRowByRow` fallback path in `readWithConstraint()`. For full ADBC performance (batch spilling), the `doReadRecords` override needs to be restored with proper code.

3. **The Dockerfile** copies native Go driver libs from the existing deployed image (`471112813056.dkr.ecr.eu-north-1.amazonaws.com/snowflake-adbc-lambda:latest`). If you need to rebuild the native libs, use the Go build instructions in SNOWFLAKE_ADBC_POC.md.

4. **Recovered files**: The ADBC code was recovered from:
   - Claude session logs (SNOWFLAKE_ADBC_POC.md — original)
   - Lambda container image decompilation (Java classes → CFR/Procyon)
   - The decompiled code has minor issues (variable names, missing comments, one broken method)

### Other Workspaces (symlinks restored)

```
/Users/gaddamdg/workspace/
├── AppFlowFix/                     → Brazil workspace for OOM fix CR
├── aws-athena-query-federation/    → GitHub repo, branch: adbc-snowflake-poc
├── glue -> /Volumes/BrazilWorkspace/glue
├── glue-docs-workspace -> /Volumes/BrazilWorkspace/glue-docs-workspace
├── glue-sdc -> /Volumes/BrazilWorkspace/glue-sdc
├── Appflow_SandstoneConfigurationServiceLambda -> /Volumes/BrazilWorkspace/...
└── SandstoneBridgeServiceSyncLambda -> /Volumes/BrazilWorkspace/...
```

### AWS Accounts

| Account | Purpose | Creds |
|---------|---------|-------|
| 471112813056 | Lambda deployment (ADBC) | `ada credentials update --account 471112813056 --role Admin --provider isengard --once` |
| 932332171447 | Customer account (Glue catalog, secrets) | |
| 610409754567 | Connector service API calls | |

### Testing

```bash
# Refresh creds
ada credentials update --account 471112813056 --role Admin --provider isengard --once

# Check Lambda logs
aws logs filter-log-events --log-group-name "/aws/lambda/3ef430b0ccbf-SNOWFLAKE-ADBC" --start-time $(date -v-10M +%s000) --filter-pattern "ERROR OR REPORT OR ADBC" --region eu-north-1

# Check execution time
aws logs filter-log-events --log-group-name "/aws/lambda/3ef430b0ccbf-SNOWFLAKE-ADBC" --start-time $(date -v-10M +%s000) --filter-pattern "REPORT" --region eu-north-1
```

### Latest Test Results (Jul 9, 2026)

Query: `SELECT * FROM tpcds100.catalog_sales` via ADBC native Go driver

| Metric | Value |
|--------|-------|
| ReadRecords Duration | 57-80s per split |
| Peak Memory | 3.5-3.7 GB |
| Lambda Memory | 5120 MB |
| Errors | None |
| Cold Start (init timeout) | ~10s (then retried successfully) |

---

## TODO / Next Steps

1. **Restore `doReadRecords` override** with `AdbcBlockSpiller` for full batch-copy performance (currently using row-by-row fallback via S3BlockSpiller)
2. **Run comparison benchmarks** — ADBC vs JDBC side-by-side with the same query
3. **Test with `customer` table** (2M rows) to compare directly with POC doc benchmarks
4. **Push `adbc-snowflake-poc` branch** to GitHub fork for safekeeping
5. **Commit changes** in `AwsAppFlowConnector-Snowflake` Brazil package
