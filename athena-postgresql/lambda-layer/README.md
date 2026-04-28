# ADBC Native PostgreSQL Driver - Lambda Layer

This Lambda layer provides the native ADBC PostgreSQL driver for use with `adbc-driver-jni` in Java Lambda functions. It enables direct Arrow batch transfer from PostgreSQL using the binary COPY protocol, bypassing JDBC entirely.

## Contents

| Library | Description |
|---|---|
| `libadbc_driver_postgresql.so` | Native ADBC PostgreSQL driver |
| `libadbc_driver_manager.so` | ADBC driver manager |
| `libpq.so.5` | PostgreSQL client library (minimal build, no LDAP/GSSAPI/ICU) |

## Requirements

- **Lambda runtime:** `java21` (AL2023, glibc 2.34+)
- **Architecture:** `x86_64`
- **Java dependency:** `org.apache.arrow.adbc:adbc-driver-jni:0.23.0`

## Build

```bash
cd lambda-layer
./build-layer.sh
```

This uses Docker with an AL2023 base image to:
1. Build PostgreSQL `libpq` from source with minimal dependencies (`--without-ldap --without-gssapi --without-icu`)
2. Build `libadbc_driver_postgresql.so` from the Apache Arrow ADBC source
3. Package everything into `output/adbc-native-layer.zip`

## Deploy

```bash
./build-layer.sh --deploy us-east-1
```

Or manually:
```bash
aws lambda publish-layer-version \
    --layer-name adbc-native-postgresql \
    --compatible-runtimes java21 \
    --compatible-architectures x86_64 \
    --zip-file fileb://output/adbc-native-layer.zip \
    --region us-east-1
```

## Usage in Java

```java
// The layer makes libraries available at /opt/lib/
// Lambda automatically adds /opt/lib to LD_LIBRARY_PATH

JniDriver driver = new JniDriver(allocator);
Map<String, Object> params = new HashMap<>();
params.put("jni.driver", "/opt/lib/libadbc_driver_postgresql.so");
params.put("uri", "postgresql://user:pass@host:5432/db");

AdbcDatabase db = driver.open(params);
AdbcConnection conn = db.connect();
AdbcStatement stmt = conn.createStatement();
stmt.setSqlQuery("SELECT * FROM orders");

// Returns Arrow batches directly from PostgreSQL binary COPY protocol
AdbcStatement.QueryResult result = stmt.executeQuery();
ArrowReader reader = result.getReader();
while (reader.loadNextBatch()) {
    VectorSchemaRoot batch = reader.getVectorSchemaRoot();
    // Process Arrow batch directly - no JDBC conversion overhead
}
```
