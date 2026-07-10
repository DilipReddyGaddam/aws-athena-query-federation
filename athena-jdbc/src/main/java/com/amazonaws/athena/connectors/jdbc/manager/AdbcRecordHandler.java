/*-
 * #%L
 * Amazon Athena JDBC Connector
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
/*
 * Decompiled with CFR 0.152.
 */
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import com.amazonaws.athena.connectors.jdbc.connection.AdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.manager.AdbcBlockSpiller;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

public abstract class AdbcRecordHandler
extends RecordHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(AdbcRecordHandler.class);
    private final AdbcConnectionFactory adbcConnectionFactory;
    private final DatabaseConnectionConfig databaseConnectionConfig;
    private final S3Client s3Client;
    private final AthenaClient athenaClient;

    protected AdbcRecordHandler(S3Client amazonS3, SecretsManagerClient secretsManager, AthenaClient athena, DatabaseConnectionConfig databaseConnectionConfig, AdbcConnectionFactory adbcConnectionFactory, Map<String, String> configOptions) {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig.getEngine(), configOptions);
        this.adbcConnectionFactory = Validate.notNull(adbcConnectionFactory, "adbcConnectionFactory must not be null", new Object[0]);
        this.databaseConnectionConfig = Validate.notNull(databaseConnectionConfig, "databaseConnectionConfig must not be null", new Object[0]);
        this.s3Client = amazonS3;
        this.athenaClient = athena;
    }

    protected AdbcConnectionFactory getAdbcConnectionFactory() {
        return this.adbcConnectionFactory;
    }

    protected DatabaseConnectionConfig getDatabaseConnectionConfig() {
        return this.databaseConnectionConfig;
    }

    @Override
    public String getDatabaseConnectionSecret() {
        if (Objects.nonNull(this.databaseConnectionConfig)) {
            return this.databaseConnectionConfig.getSecret();
        }
        return null;
    }

    protected String getNativeDriverPath() {
        return null;
    }

    protected Map<String, String> getNativeConnectionParams() {
        return Collections.emptyMap();
    }

    // Note: doReadRecords is intentionally not overridden here.
    // The base RecordHandler.doReadRecords() creates an S3BlockSpiller and calls readWithConstraint().
    // readWithConstraint() handles both batch (AdbcBlockSpiller) and row-by-row (S3BlockSpiller) paths.

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    @Override
    public void readWithConstraint(BlockSpiller blockSpiller, ReadRecordsRequest readRecordsRequest, QueryStatusChecker queryStatusChecker) throws Exception {
        LOGGER.info("{}: ADBC readWithConstraint: {}, table {}", readRecordsRequest.getQueryId(), readRecordsRequest.getCatalogName(), readRecordsRequest.getTableName());
        String sql = this.buildSqlForAdbc(readRecordsRequest);
        LOGGER.info("ADBC executing SQL: {}", (Object)sql);
        Schema expectedSchema = readRecordsRequest.getSchema();
        Map<String, String> partitionValues = readRecordsRequest.getSplit().getProperties();
        String nativeDriverPath = this.getNativeDriverPath();
        boolean useBatchPath = blockSpiller instanceof AdbcBlockSpiller;
        try (RootAllocator adbcAllocator = new RootAllocator();){
            AdbcConnection conn;
            long connectStart = System.currentTimeMillis();
            if (nativeDriverPath != null) {
                LOGGER.info("Using native ADBC driver at {}", (Object)nativeDriverPath);
                conn = this.adbcConnectionFactory.getNativeConnection(this.getCredentialProvider(this.getRequestOverrideConfig(readRecordsRequest)), adbcAllocator, nativeDriverPath, this.getNativeConnectionParams());
            } else {
                LOGGER.info("Using ADBC JDBC adapter");
                conn = this.adbcConnectionFactory.getConnection(this.getCredentialProvider(this.getRequestOverrideConfig(readRecordsRequest)), adbcAllocator);
            }
            long connectMs = System.currentTimeMillis() - connectStart;
            try (AdbcConnection adbcConnection = conn;
                 AdbcStatement stmt = conn.createStatement();){
                stmt.setSqlQuery(sql);
                this.logMemory("BEFORE_QUERY");
                long queryStart = System.currentTimeMillis();
                AdbcStatement.QueryResult queryResult = stmt.executeQuery();
                ArrowReader reader = queryResult.getReader();
                this.logMemory("AFTER_EXECUTE");
                try {
                    int totalRows = 0;
                    long firstBatchMs = 0L;
                    long spillMs = 0L;
                    while (reader.loadNextBatch()) {
                        if (totalRows == 0) {
                            firstBatchMs = System.currentTimeMillis() - queryStart;
                            this.logMemory("FIRST_BATCH");
                        }
                        if (!queryStatusChecker.isQueryRunning()) {
                            LOGGER.info("Query no longer running, stopping read.");
                            return;
                        }
                        VectorSchemaRoot batch = reader.getVectorSchemaRoot();
                        long spillStart = System.currentTimeMillis();
                        if (useBatchPath) {
                            ((AdbcBlockSpiller)blockSpiller).writeBatch(batch, partitionValues);
                        } else {
                            this.writeRowByRow(batch, blockSpiller, expectedSchema, partitionValues, queryStatusChecker);
                        }
                        spillMs += System.currentTimeMillis() - spillStart;
                        totalRows += batch.getRowCount();
                    }
                    long totalReadMs = System.currentTimeMillis() - queryStart;
                    this.logMemory("AFTER_READ_LOOP");
                    LOGGER.info("PERF: connect={}ms firstBatch={}ms totalRead={}ms spill={}ms rows={} batch={}", connectMs, firstBatchMs, totalReadMs, spillMs, totalRows, useBatchPath);
                }
                finally {
                    try {
                        reader.close();
                    }
                    catch (Exception e) {
                        LOGGER.warn("Error closing ArrowReader: {}", (Object)e.getMessage());
                    }
                    this.logMemory("AFTER_READER_CLOSE");
                }
            }
        }
    }

    private void writeRowByRow(VectorSchemaRoot batch, BlockSpiller spiller, Schema expectedSchema, Map<String, String> partitionValues, QueryStatusChecker queryStatusChecker) {
        int rowCount = batch.getRowCount();
        int row = 0;
        while (row < rowCount) {
            if (!queryStatusChecker.isQueryRunning()) {
                return;
            }
            int currentRow = row++;
            spiller.writeRows((block, rowNum) -> {
                boolean matched = true;
                for (Field field : expectedSchema.getFields()) {
                    String fieldName = field.getName();
                    if (partitionValues.containsKey(fieldName)) {
                        matched &= block.offerValue(fieldName, rowNum, partitionValues.get(fieldName));
                        continue;
                    }
                    FieldVector sourceVector = batch.getVector(fieldName);
                    if (sourceVector == null || sourceVector.isNull(currentRow)) {
                        matched &= block.offerValue(fieldName, rowNum, null);
                        continue;
                    }
                    Object value = sourceVector.getObject(currentRow);
                    FieldVector targetVector = block.getFieldVector(fieldName);
                    boolean targetIsList = field.getType().getTypeID() == ArrowType.ArrowTypeID.List;
                    boolean targetIsDecimal = targetVector instanceof DecimalVector;
                    if (targetIsDecimal) {
                        DecimalVector dv = (DecimalVector)targetVector;
                        BigDecimal bd = value instanceof BigDecimal ? (BigDecimal)value : new BigDecimal(value.toString());
                        dv.setSafe(rowNum, bd.setScale(dv.getScale(), RoundingMode.HALF_UP));
                        continue;
                    }
                    if (targetIsList && value instanceof Text) {
                        value = AdbcBlockSpiller.parsePostgresArray(value.toString());
                        BlockUtils.setComplexValue(targetVector, rowNum, FieldResolver.DEFAULT, value);
                        continue;
                    }
                    if (targetIsList) {
                        BlockUtils.setComplexValue(targetVector, rowNum, FieldResolver.DEFAULT, value);
                        continue;
                    }
                    if (value instanceof Text) {
                        value = value.toString();
                    }
                    matched &= block.offerValue(fieldName, rowNum, value);
                }
                return matched ? 1 : 0;
            });
        }
    }

    protected abstract String buildSqlForAdbc(ReadRecordsRequest var1);

    private void logMemory(String phase) {
        try {
            String line;
            Runtime rt = Runtime.getRuntime();
            long heapUsedMB = (rt.totalMemory() - rt.freeMemory()) / 0x100000L;
            long heapMaxMB = rt.maxMemory() / 0x100000L;
            long vmRssMB = -1L;
            long vmSizeMB = -1L;
            BufferedReader reader = new BufferedReader(new FileReader("/proc/self/status"));
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("VmRSS:")) {
                    vmRssMB = Long.parseLong(line.replaceAll("[^0-9]", "")) / 1024L;
                    continue;
                }
                if (!line.startsWith("VmSize:")) continue;
                vmSizeMB = Long.parseLong(line.replaceAll("[^0-9]", "")) / 1024L;
            }
            reader.close();
            LOGGER.info("MEMORY[{}]: heapUsed={}MB heapMax={}MB vmRSS={}MB vmSize={}MB nonHeap={}MB", phase, heapUsedMB, heapMaxMB, vmRssMB, vmSizeMB, vmRssMB - heapUsedMB);
            File tmpDir = new File("/tmp");
            long tmpTotalMB = tmpDir.getTotalSpace() / 0x100000L;
            long tmpFreeMB = tmpDir.getFreeSpace() / 0x100000L;
            long tmpUsedMB = tmpTotalMB - tmpFreeMB;
            LOGGER.info("DISK[{}]: /tmp used={}MB free={}MB total={}MB", phase, tmpUsedMB, tmpFreeMB, tmpTotalMB);
        }
        catch (Exception e) {
            LOGGER.warn("Failed to log memory at {}: {}", (Object)phase, (Object)e.getMessage());
        }
    }
}
