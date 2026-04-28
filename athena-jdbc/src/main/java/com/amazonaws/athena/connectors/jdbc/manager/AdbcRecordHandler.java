/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.connection.AdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.amazonaws.athena.connector.lambda.handlers.AthenaExceptionFilter.ATHENA_EXCEPTION_FILTER;

/**
 * Abstract record handler that uses the Arrow JDBC adapter directly with a custom
 * {@link JdbcToArrowConfig} for batch-level Arrow data transfer.
 *
 * <p>Uses {@link JdbcToArrow#sqlToArrowVectorIterator(ResultSet, JdbcToArrowConfig)} with
 * configured {@code arraySubTypesByColumnName} and {@code explicitTypesByColumnName} to handle
 * database-specific types (arrays, uuid, json, etc.) without SQL workarounds.</p>
 *
 * <p>Subclasses must implement {@link #buildSqlForAdbc(ReadRecordsRequest)} to provide
 * the SQL query string for their specific database.</p>
 */
public abstract class AdbcRecordHandler
        extends RecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AdbcRecordHandler.class);

    private final AdbcConnectionFactory adbcConnectionFactory;
    private final DatabaseConnectionConfig databaseConnectionConfig;
    private final S3Client s3Client;
    private final AthenaClient athenaClient;

    protected AdbcRecordHandler(
            S3Client amazonS3,
            SecretsManagerClient secretsManager,
            AthenaClient athena,
            DatabaseConnectionConfig databaseConnectionConfig,
            AdbcConnectionFactory adbcConnectionFactory,
            Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig.getEngine(), configOptions);
        this.adbcConnectionFactory = Validate.notNull(adbcConnectionFactory, "adbcConnectionFactory must not be null");
        this.databaseConnectionConfig = Validate.notNull(databaseConnectionConfig, "databaseConnectionConfig must not be null");
        this.s3Client = amazonS3;
        this.athenaClient = athena;
    }

    protected AdbcConnectionFactory getAdbcConnectionFactory()
    {
        return adbcConnectionFactory;
    }

    protected DatabaseConnectionConfig getDatabaseConnectionConfig()
    {
        return databaseConnectionConfig;
    }

    @Override
    public String getDatabaseConnectionSecret()
    {
        if (Objects.nonNull(databaseConnectionConfig)) {
            return databaseConnectionConfig.getSecret();
        }
        return null;
    }

    /**
     * Returns path to native ADBC driver .so. If non-null, uses native driver via JNI.
     * If null (default), uses Arrow JDBC adapter. Subclasses override to enable native driver.
     */
    protected String getNativeDriverPath()
    {
        return null;
    }

    /**
     * Uses the Arrow JDBC adapter directly with a custom {@link JdbcToArrowConfig}.
     */
    @Override
    public RecordResponse doReadRecords(BlockAllocator allocator, ReadRecordsRequest request)
            throws Exception
    {
        LOGGER.info("doReadRecords (batch): {}:{}", request.getSchema(),
                request.getSplit().getSpillLocation());

        SpillConfig spillConfig = getSpillConfig(request);
        FederatedIdentity identity = request.getIdentity();
        AwsRequestOverrideConfiguration overrideConfig = getRequestOverrideConfig(identity.getConfigOptions());
        S3Client s3 = getS3Client(overrideConfig, this.s3Client);
        AthenaClient athena = getAthenaClient(overrideConfig, this.athenaClient);
        ThrottlingInvoker athenaInvoker = ThrottlingInvoker.newDefaultBuilder(ATHENA_EXCEPTION_FILTER, configOptions).build();

        try (ConstraintEvaluator evaluator = new ConstraintEvaluator(allocator,
                request.getSchema(), request.getConstraints());
                AdbcBlockSpiller spiller = new AdbcBlockSpiller(s3, spillConfig, allocator,
                        request.getSchema(), evaluator);
                QueryStatusChecker checker = new QueryStatusChecker(athena, athenaInvoker,
                        request.getQueryId())) {
            String sql = buildSqlForAdbc(request);
            Map<String, String> partitionValues = request.getSplit().getProperties();
            String nativeDriverPath = getNativeDriverPath();

            if (nativeDriverPath != null) {
                try {
                    LOGGER.info("Native ADBC executing SQL: {}", sql);
                    executeWithNativeDriver(request, sql, nativeDriverPath, spiller, checker, partitionValues);
                }
                catch (NoClassDefFoundError | UnsatisfiedLinkError e) {
                    LOGGER.warn("Native driver JNI loading failed, falling back to Arrow JDBC: {}", e.getMessage());
                    executeWithArrowJdbc(request, sql, spiller, checker, partitionValues);
                }
            }
            else {
                LOGGER.info("Arrow JDBC executing SQL: {}", sql);
                executeWithArrowJdbc(request, sql, spiller, checker, partitionValues);
            }

            if (!spiller.spilled()) {
                return new ReadRecordsResponse(request.getCatalogName(), spiller.getBlock());
            }
            else {
                return new RemoteReadRecordsResponse(request.getCatalogName(),
                        request.getSchema(), spiller.getSpillLocations(), spillConfig.getEncryptionKey());
            }
        }
    }

    private void executeWithNativeDriver(ReadRecordsRequest request, String sql, String nativeDriverPath,
            AdbcBlockSpiller spiller, QueryStatusChecker checker, Map<String, String> partitionValues)
            throws Exception
    {
        try (BufferAllocator adbcAllocator = new RootAllocator();
                AdbcConnection conn = adbcConnectionFactory.getNativeConnection(
                        getCredentialProvider(getRequestOverrideConfig(request)), adbcAllocator, nativeDriverPath);
                AdbcStatement stmt = conn.createStatement()) {
            stmt.setSqlQuery(sql);
            AdbcStatement.QueryResult queryResult = stmt.executeQuery();
            ArrowReader reader = queryResult.getReader();
            try {
                int totalRows = 0;
                while (reader.loadNextBatch()) {
                    if (!checker.isQueryRunning()) {
                        LOGGER.info("Query no longer running, stopping read.");
                        break;
                    }
                    VectorSchemaRoot batch = reader.getVectorSchemaRoot();
                    LOGGER.debug("Native batch: {} rows", batch.getRowCount());
                    spiller.writeBatch(batch, partitionValues);
                    totalRows += batch.getRowCount();
                }
                LOGGER.info("{} rows via native ADBC.", totalRows);
            }
            finally {
                // Close only the reader; QueryResult.close() would try to close the
                // reader again, causing "ArrowArrayStream is already closed" on the
                // native C stream. The reader close releases all underlying resources.
                try {
                    reader.close();
                }
                catch (Exception e) {
                    LOGGER.warn("Error closing ArrowReader (expected for native driver): {}", e.getMessage());
                }
            }
        }
    }

    private void executeWithArrowJdbc(ReadRecordsRequest request, String sql,
            AdbcBlockSpiller spiller, QueryStatusChecker checker, Map<String, String> partitionValues)
            throws Exception
    {
        try (BufferAllocator arrowAllocator = new RootAllocator();
                Connection jdbcConn = adbcConnectionFactory.getJdbcConnection(
                        getCredentialProvider(getRequestOverrideConfig(request)))) {
            JdbcToArrowConfig arrowConfig = buildArrowConfig(arrowAllocator, request.getSchema(),
                    request.getSplit().getProperties());
            try (Statement stmt = jdbcConn.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    ResultSet rs = stmt.executeQuery(sql);
                    ArrowVectorIterator iterator = JdbcToArrow.sqlToArrowVectorIterator(rs, arrowConfig)) {
                int totalRows = 0;
                while (iterator.hasNext()) {
                    if (!checker.isQueryRunning()) {
                        LOGGER.info("Query no longer running, stopping read.");
                        break;
                    }
                    try (VectorSchemaRoot batch = iterator.next()) {
                        LOGGER.debug("Batch received with {} rows", batch.getRowCount());
                        spiller.writeBatch(batch, partitionValues);
                        totalRows += batch.getRowCount();
                    }
                }
                LOGGER.info("{} rows returned via Arrow JDBC batch transfer.", totalRows);
            }
        }
    }

    /**
     * Fallback row-by-row implementation kept for interface compatibility.
     */
    @Override
    public void readWithConstraint(BlockSpiller blockSpiller, ReadRecordsRequest readRecordsRequest,
            QueryStatusChecker queryStatusChecker) throws Exception
    {
        LOGGER.info("{}: Arrow JDBC fallback readWithConstraint: {}, table {}", readRecordsRequest.getQueryId(),
                readRecordsRequest.getCatalogName(), readRecordsRequest.getTableName());

        String sql = buildSqlForAdbc(readRecordsRequest);
        LOGGER.info("Arrow JDBC executing SQL: {}", sql);

        try (BufferAllocator arrowAllocator = new RootAllocator();
                Connection jdbcConn = adbcConnectionFactory.getJdbcConnection(
                        getCredentialProvider(getRequestOverrideConfig(readRecordsRequest)))) {
            JdbcToArrowConfig arrowConfig = buildArrowConfig(arrowAllocator, readRecordsRequest.getSchema(),
                    readRecordsRequest.getSplit().getProperties());

            try (Statement stmt = jdbcConn.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    ResultSet rs = stmt.executeQuery(sql);
                    ArrowVectorIterator iterator = JdbcToArrow.sqlToArrowVectorIterator(rs, arrowConfig)) {
                Schema expectedSchema = readRecordsRequest.getSchema();
                Map<String, String> partitionValues = readRecordsRequest.getSplit().getProperties();

                int totalRows = 0;
                while (iterator.hasNext()) {
                    if (!queryStatusChecker.isQueryRunning()) {
                        LOGGER.info("Query no longer running, stopping read.");
                        return;
                    }

                    try (VectorSchemaRoot batch = iterator.next()) {
                        LOGGER.debug("Batch received with {} rows", batch.getRowCount());
                        if (blockSpiller instanceof AdbcBlockSpiller) {
                            ((AdbcBlockSpiller) blockSpiller).writeBatch(batch, partitionValues);
                        }
                        else {
                            writeRowByRow(batch, blockSpiller, expectedSchema, partitionValues,
                                    queryStatusChecker);
                        }
                        totalRows += batch.getRowCount();
                    }
                }
                LOGGER.info("{} rows returned via Arrow JDBC.", totalRows);
            }
        }
    }

    /**
     * Builds a {@link JdbcToArrowConfig} with type mappings derived from the request schema.
     * <ul>
     *   <li>List fields → arraySubTypesByColumnName (maps column to its element JDBC type)</li>
     *   <li>Utf8 fields → explicitTypesByColumnName as VARCHAR (handles uuid/json/jsonb mapped to Utf8)</li>
     * </ul>
     * Subclasses can override to add database-specific mappings.
     */
    protected JdbcToArrowConfig buildArrowConfig(BufferAllocator allocator, Schema schema,
            Map<String, String> partitionColumns)
    {
        Map<String, JdbcFieldInfo> arraySubTypes = new HashMap<>();
        Map<String, JdbcFieldInfo> explicitTypes = new HashMap<>();

        for (Field field : schema.getFields()) {
            if (partitionColumns.containsKey(field.getName())) {
                continue;
            }
            ArrowType type = field.getType();
            if (type.getTypeID() == ArrowType.ArrowTypeID.List && !field.getChildren().isEmpty()) {
                ArrowType childType = field.getChildren().get(0).getType();
                int jdbcType = arrowTypeToJdbcType(childType);
                arraySubTypes.put(field.getName(), new JdbcFieldInfo(jdbcType));
                LOGGER.debug("Array sub-type mapping: {} -> JDBC type {}", field.getName(), jdbcType);
            }
            else if (type.getTypeID() == ArrowType.ArrowTypeID.Utf8) {
                // Map Utf8 columns explicitly as VARCHAR to handle Types.OTHER (uuid, json, etc.)
                explicitTypes.put(field.getName(), new JdbcFieldInfo(Types.VARCHAR));
            }
        }

        JdbcToArrowConfigBuilder builder = new JdbcToArrowConfigBuilder()
                .setAllocator(allocator)
                .setCalendar(JdbcToArrowUtils.getUtcCalendar())
                .setTargetBatchSize(1024);

        if (!arraySubTypes.isEmpty()) {
            builder.setArraySubTypeByColumnNameMap(arraySubTypes);
            LOGGER.info("Configured {} array sub-type mappings", arraySubTypes.size());
        }
        if (!explicitTypes.isEmpty()) {
            builder.setExplicitTypesByColumnName(explicitTypes);
            LOGGER.info("Configured {} explicit type mappings for Utf8 columns", explicitTypes.size());
        }

        return builder.build();
    }

    /**
     * Maps an Arrow type to the corresponding java.sql.Types constant.
     */
    protected static int arrowTypeToJdbcType(ArrowType arrowType)
    {
        if (arrowType instanceof ArrowType.Bool) {
            return Types.BOOLEAN;
        }
        else if (arrowType instanceof ArrowType.Int) {
            int bitWidth = ((ArrowType.Int) arrowType).getBitWidth();
            if (bitWidth <= 16) {
                return Types.SMALLINT;
            }
            else if (bitWidth <= 32) {
                return Types.INTEGER;
            }
            return Types.BIGINT;
        }
        else if (arrowType instanceof ArrowType.FloatingPoint) {
            FloatingPointPrecision precision = ((ArrowType.FloatingPoint) arrowType).getPrecision();
            return precision == FloatingPointPrecision.SINGLE ? Types.REAL : Types.DOUBLE;
        }
        else if (arrowType instanceof ArrowType.Decimal) {
            return Types.DECIMAL;
        }
        else if (arrowType instanceof ArrowType.Date) {
            return Types.DATE;
        }
        else if (arrowType instanceof ArrowType.Timestamp) {
            return Types.TIMESTAMP;
        }
        else if (arrowType instanceof ArrowType.Binary || arrowType instanceof ArrowType.LargeBinary) {
            return Types.BINARY;
        }
        return Types.VARCHAR;
    }

    /**
     * Row-by-row fallback for non-AdbcBlockSpiller (e.g. S3BlockSpiller).
     */
    private void writeRowByRow(VectorSchemaRoot batch, BlockSpiller spiller,
            Schema expectedSchema, Map<String, String> partitionValues,
            QueryStatusChecker queryStatusChecker)
    {
        int rowCount = batch.getRowCount();
        for (int row = 0; row < rowCount; row++) {
            if (!queryStatusChecker.isQueryRunning()) {
                return;
            }

            final int currentRow = row;
            spiller.writeRows((com.amazonaws.athena.connector.lambda.data.Block block, int rowNum) -> {
                boolean matched = true;
                for (Field field : expectedSchema.getFields()) {
                    String fieldName = field.getName();

                    if (partitionValues.containsKey(fieldName)) {
                        matched &= block.offerValue(fieldName, rowNum, partitionValues.get(fieldName));
                        continue;
                    }

                    org.apache.arrow.vector.FieldVector sourceVector = batch.getVector(fieldName);
                    if (sourceVector != null) {
                        Object value = sourceVector.isNull(currentRow) ? null : sourceVector.getObject(currentRow);
                        matched &= block.offerValue(fieldName, rowNum, value);
                    }
                }
                return matched ? 1 : 0;
            });
        }
    }

    /**
     * Builds the SQL query string for execution.
     * Subclasses should use their database-specific query builder to generate the SQL.
     */
    protected abstract String buildSqlForAdbc(ReadRecordsRequest request);
}
