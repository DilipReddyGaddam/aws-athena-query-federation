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
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.Map;
import java.util.Objects;

import static com.amazonaws.athena.connector.lambda.handlers.AthenaExceptionFilter.ATHENA_EXCEPTION_FILTER;

/**
 * Abstract ADBC-based record handler that uses Arrow Database Connectivity to fetch data
 * natively as Arrow columnar batches, eliminating the row-by-row JDBC ResultSet to Arrow
 * conversion overhead.
 *
 * <p>Overrides {@link RecordHandler#doReadRecords(BlockAllocator, ReadRecordsRequest)} to use
 * {@link AdbcBlockSpiller} for batch-level vector transfers instead of the row-by-row
 * {@link com.amazonaws.athena.connector.lambda.data.S3BlockSpiller} approach.</p>
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
    // Stored separately because RecordHandler's fields are private
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
     * Overrides the default doReadRecords to use {@link AdbcBlockSpiller} for batch-level
     * vector transfers instead of the row-by-row S3BlockSpiller approach.
     *
     * <p>This method:</p>
     * <ol>
     *   <li>Creates an {@link AdbcBlockSpiller} instead of S3BlockSpiller</li>
     *   <li>Opens an ADBC connection and executes the SQL query</li>
     *   <li>Writes each Arrow batch directly via {@link AdbcBlockSpiller#writeBatch}</li>
     *   <li>Returns the appropriate response (inline or spilled)</li>
     * </ol>
     */
    @Override
    public RecordResponse doReadRecords(BlockAllocator allocator, ReadRecordsRequest request)
            throws Exception
    {
        LOGGER.info("doReadRecords (ADBC batch): {}:{}", request.getSchema(),
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
            LOGGER.info("ADBC executing SQL: {}", sql);

            try (BufferAllocator adbcAllocator = new RootAllocator()) {
                try (AdbcConnection conn = adbcConnectionFactory.getConnection(
                        getCredentialProvider(getRequestOverrideConfig(request)), adbcAllocator)) {
                    try (AdbcStatement stmt = conn.createStatement()) {
                        stmt.setSqlQuery(sql);

                        try (AdbcStatement.QueryResult queryResult = stmt.executeQuery();
                                ArrowReader reader = queryResult.getReader()) {
                            Map<String, String> partitionValues = request.getSplit().getProperties();
                            int totalRows = 0;

                            while (reader.loadNextBatch()) {
                                if (!checker.isQueryRunning()) {
                                    LOGGER.info("Query no longer running, stopping ADBC read.");
                                    break;
                                }

                                VectorSchemaRoot batch = reader.getVectorSchemaRoot();
                                LOGGER.debug("ADBC batch received with {} rows", batch.getRowCount());
                                spiller.writeBatch(batch, partitionValues);
                                totalRows += batch.getRowCount();
                            }
                            LOGGER.info("{} rows returned by database via ADBC batch transfer.", totalRows);
                        }
                    }
                }
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

    /**
     * Fallback row-by-row implementation. This is not called when {@link #doReadRecords} is
     * used (which creates its own AdbcBlockSpiller), but is kept for interface compatibility.
     */
    @Override
    public void readWithConstraint(BlockSpiller blockSpiller, ReadRecordsRequest readRecordsRequest,
            QueryStatusChecker queryStatusChecker) throws Exception
    {
        LOGGER.info("{}: ADBC fallback readWithConstraint: {}, table {}", readRecordsRequest.getQueryId(),
                readRecordsRequest.getCatalogName(), readRecordsRequest.getTableName());

        String sql = buildSqlForAdbc(readRecordsRequest);
        LOGGER.info("ADBC executing SQL: {}", sql);

        try (BufferAllocator adbcAllocator = new RootAllocator()) {
            try (AdbcConnection adbcConnection = adbcConnectionFactory.getConnection(
                    getCredentialProvider(getRequestOverrideConfig(readRecordsRequest)), adbcAllocator)) {
                try (AdbcStatement stmt = adbcConnection.createStatement()) {
                    stmt.setSqlQuery(sql);

                    try (AdbcStatement.QueryResult queryResult = stmt.executeQuery();
                            ArrowReader reader = queryResult.getReader()) {
                        Schema expectedSchema = readRecordsRequest.getSchema();
                        Map<String, String> partitionValues = readRecordsRequest.getSplit().getProperties();

                        int totalRows = 0;
                        while (reader.loadNextBatch()) {
                            if (!queryStatusChecker.isQueryRunning()) {
                                LOGGER.info("Query no longer running, stopping ADBC read.");
                                return;
                            }

                            VectorSchemaRoot batch = reader.getVectorSchemaRoot();
                            int batchRowCount = batch.getRowCount();
                            LOGGER.debug("ADBC batch received with {} rows", batchRowCount);

                            // If spiller is an AdbcBlockSpiller, use batch path
                            if (blockSpiller instanceof AdbcBlockSpiller) {
                                ((AdbcBlockSpiller) blockSpiller).writeBatch(batch, partitionValues);
                            }
                            else {
                                writeRowByRow(batch, blockSpiller, expectedSchema, partitionValues,
                                        queryStatusChecker);
                            }
                            totalRows += batchRowCount;
                        }
                        LOGGER.info("{} rows returned by database via ADBC.", totalRows);
                    }
                }
            }
        }
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
                for (org.apache.arrow.vector.types.pojo.Field field : expectedSchema.getFields()) {
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
     * Builds the SQL query string for ADBC execution.
     * Subclasses should use their database-specific query builder to generate the SQL.
     *
     * @param request the read records request containing table, schema, constraints, and split info
     * @return SQL string to execute via ADBC
     */
    protected abstract String buildSqlForAdbc(ReadRecordsRequest request);
}
