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
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.connection.AdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

public class AdbcRecordHandlerTest
{
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_SECRET = "testSecret";
    private static final String TEST_QUERY_ID = "testQueryId";
    private static final String TEST_SCHEMA_NAME = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String TEST_COL1 = "testCol1";
    private static final String TEST_COL2 = "testCol2";
    private static final String TEST_PARTITION_COL = "testPartitionCol";
    private static final String TEST_PARTITION_VALUE = "testPartitionValue";
    private static final String CONNECTION_STRING = "fakedatabase://jdbc:fakedatabase://hostname/${" + TEST_SECRET + "}";
    private static final String TEST_SQL = "SELECT * FROM testTable";

    private AdbcRecordHandler adbcRecordHandler;
    private AdbcConnectionFactory adbcConnectionFactory;
    private Connection mockJdbcConnection;
    private Statement mockJdbcStatement;
    private ResultSet mockResultSet;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private QueryStatusChecker queryStatusChecker;
    private FederatedIdentity federatedIdentity;

    @Before
    public void setup() throws Exception
    {
        this.adbcConnectionFactory = Mockito.mock(AdbcConnectionFactory.class);
        this.mockJdbcConnection = Mockito.mock(Connection.class);
        this.mockJdbcStatement = Mockito.mock(Statement.class);
        this.mockResultSet = Mockito.mock(ResultSet.class);
        this.amazonS3 = Mockito.mock(S3Client.class);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        this.queryStatusChecker = Mockito.mock(QueryStatusChecker.class);
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);

        when(this.queryStatusChecker.isQueryRunning()).thenReturn(true);
        when(this.secretsManager.getSecretValue(Mockito.eq(GetSecretValueRequest.builder().secretId(TEST_SECRET).build())))
                .thenReturn(GetSecretValueResponse.builder()
                        .secretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}")
                        .build());

        when(this.adbcConnectionFactory.getJdbcConnection(any())).thenReturn(this.mockJdbcConnection);
        when(this.mockJdbcConnection.createStatement(anyInt(), anyInt())).thenReturn(this.mockJdbcStatement);
        when(this.mockJdbcStatement.executeQuery(anyString())).thenReturn(this.mockResultSet);

        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(
                TEST_CATALOG, "fakedatabase", CONNECTION_STRING, TEST_SECRET);

        this.adbcRecordHandler = new AdbcRecordHandler(
                this.amazonS3, this.secretsManager, this.athena,
                databaseConnectionConfig, this.adbcConnectionFactory, ImmutableMap.of())
        {
            @Override
            protected String buildSqlForAdbc(ReadRecordsRequest request)
            {
                return TEST_SQL;
            }
        };
    }

    @Test
    public void readWithConstraintReturnsData() throws Exception
    {
        try (BufferAllocator testAllocator = new RootAllocator()) {
            Schema fieldSchema = SchemaBuilder.newBuilder()
                    .addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build())
                    .addField(FieldBuilder.newBuilder(TEST_COL2, Types.MinorType.VARCHAR.getType()).build())
                    .addField(FieldBuilder.newBuilder(TEST_PARTITION_COL, Types.MinorType.VARCHAR.getType()).build())
                    .build();

            List<Field> batchFields = Arrays.asList(
                    Field.nullable(TEST_COL1, Types.MinorType.INT.getType()),
                    Field.nullable(TEST_COL2, Types.MinorType.VARCHAR.getType()));
            try (VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(batchFields), testAllocator)) {
                batch.setRowCount(2);
                IntVector intVector = (IntVector) batch.getVector(TEST_COL1);
                intVector.allocateNew(2);
                intVector.set(0, 1);
                intVector.set(1, 2);
                intVector.setValueCount(2);
                VarCharVector varcharVector = (VarCharVector) batch.getVector(TEST_COL2);
                varcharVector.allocateNew();
                varcharVector.set(0, "testVal1".getBytes(StandardCharsets.UTF_8));
                varcharVector.set(1, "testVal2".getBytes(StandardCharsets.UTF_8));
                varcharVector.setValueCount(2);

                ArrowVectorIterator mockIterator = Mockito.mock(ArrowVectorIterator.class);
                when(mockIterator.hasNext()).thenReturn(true, false);
                when(mockIterator.next()).thenReturn(batch);

                try (MockedStatic<JdbcToArrow> mockedJdbcToArrow = Mockito.mockStatic(JdbcToArrow.class)) {
                    mockedJdbcToArrow.when(() -> JdbcToArrow
                            .sqlToArrowVectorIterator(any(ResultSet.class), any(JdbcToArrowConfig.class)))
                            .thenReturn(mockIterator);

                    try (BlockAllocator allocator = new BlockAllocatorImpl()) {
                        ConstraintEvaluator constraintEvaluator = Mockito.mock(ConstraintEvaluator.class);
                        when(constraintEvaluator.apply(nullable(String.class), any())).thenReturn(true);
                        S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();
                        SpillConfig spillConfig = Mockito.mock(SpillConfig.class);
                        when(spillConfig.getSpillLocation()).thenReturn(s3SpillLocation);

                        try (S3BlockSpiller spiller = new S3BlockSpiller(this.amazonS3, spillConfig, allocator, fieldSchema,
                                constraintEvaluator, ImmutableMap.of())) {
                            Split split = Split.newBuilder(s3SpillLocation, null)
                                    .add(TEST_PARTITION_COL, TEST_PARTITION_VALUE).build();
                            Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(),
                                    Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
                            ReadRecordsRequest readRecordsRequest = new ReadRecordsRequest(
                                    this.federatedIdentity, TEST_CATALOG, TEST_QUERY_ID,
                                    new TableName(TEST_SCHEMA_NAME, TEST_TABLE), fieldSchema, split, constraints, 1024, 1024);
                            when(amazonS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                                    .thenReturn(PutObjectResponse.builder().build());
                            this.adbcRecordHandler.readWithConstraint(spiller, readRecordsRequest, queryStatusChecker);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void readWithConstraintRespectsQueryCancellation() throws Exception
    {
        when(this.queryStatusChecker.isQueryRunning()).thenReturn(false);

        ArrowVectorIterator mockIterator = Mockito.mock(ArrowVectorIterator.class);
        when(mockIterator.hasNext()).thenReturn(true);

        try (MockedStatic<JdbcToArrow> mockedJdbcToArrow = Mockito.mockStatic(JdbcToArrow.class)) {
            mockedJdbcToArrow.when(() -> JdbcToArrow
                    .sqlToArrowVectorIterator(any(ResultSet.class), any(JdbcToArrowConfig.class)))
                    .thenReturn(mockIterator);

            Schema fieldSchema = SchemaBuilder.newBuilder()
                    .addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build()).build();
            try (BlockAllocator allocator = new BlockAllocatorImpl()) {
                ConstraintEvaluator constraintEvaluator = Mockito.mock(ConstraintEvaluator.class);
                S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();
                SpillConfig spillConfig = Mockito.mock(SpillConfig.class);
                when(spillConfig.getSpillLocation()).thenReturn(s3SpillLocation);
                try (S3BlockSpiller spiller = new S3BlockSpiller(this.amazonS3, spillConfig, allocator, fieldSchema,
                        constraintEvaluator, ImmutableMap.of())) {
                    Split split = Split.newBuilder(s3SpillLocation, null).build();
                    Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(),
                            Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
                    ReadRecordsRequest readRecordsRequest = new ReadRecordsRequest(
                            this.federatedIdentity, TEST_CATALOG, TEST_QUERY_ID,
                            new TableName(TEST_SCHEMA_NAME, TEST_TABLE), fieldSchema, split, constraints, 1024, 1024);
                    this.adbcRecordHandler.readWithConstraint(spiller, readRecordsRequest, queryStatusChecker);
                }
            }
        }
    }

    @Test
    public void readWithConstraintHandlesEmptyResult() throws Exception
    {
        ArrowVectorIterator mockIterator = Mockito.mock(ArrowVectorIterator.class);
        when(mockIterator.hasNext()).thenReturn(false);

        try (MockedStatic<JdbcToArrow> mockedJdbcToArrow = Mockito.mockStatic(JdbcToArrow.class)) {
            mockedJdbcToArrow.when(() -> JdbcToArrow
                    .sqlToArrowVectorIterator(any(ResultSet.class), any(JdbcToArrowConfig.class)))
                    .thenReturn(mockIterator);

            Schema fieldSchema = SchemaBuilder.newBuilder()
                    .addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build()).build();
            try (BlockAllocator allocator = new BlockAllocatorImpl()) {
                ConstraintEvaluator constraintEvaluator = Mockito.mock(ConstraintEvaluator.class);
                S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();
                SpillConfig spillConfig = Mockito.mock(SpillConfig.class);
                when(spillConfig.getSpillLocation()).thenReturn(s3SpillLocation);
                try (S3BlockSpiller spiller = new S3BlockSpiller(this.amazonS3, spillConfig, allocator, fieldSchema,
                        constraintEvaluator, ImmutableMap.of())) {
                    Split split = Split.newBuilder(s3SpillLocation, null).build();
                    Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(),
                            Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
                    ReadRecordsRequest readRecordsRequest = new ReadRecordsRequest(
                            this.federatedIdentity, TEST_CATALOG, TEST_QUERY_ID,
                            new TableName(TEST_SCHEMA_NAME, TEST_TABLE), fieldSchema, split, constraints, 1024, 1024);
                    this.adbcRecordHandler.readWithConstraint(spiller, readRecordsRequest, queryStatusChecker);
                }
            }
        }
    }

    @Test
    public void doReadRecordsBatchPathReturnsData() throws Exception
    {
        try (BufferAllocator testAllocator = new RootAllocator()) {
            Schema fieldSchema = SchemaBuilder.newBuilder()
                    .addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build())
                    .addField(FieldBuilder.newBuilder(TEST_COL2, Types.MinorType.VARCHAR.getType()).build())
                    .build();
            List<Field> batchFields = Arrays.asList(
                    Field.nullable(TEST_COL1, Types.MinorType.INT.getType()),
                    Field.nullable(TEST_COL2, Types.MinorType.VARCHAR.getType()));
            try (VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(batchFields), testAllocator)) {
                batch.setRowCount(2);
                IntVector intVector = (IntVector) batch.getVector(TEST_COL1);
                intVector.allocateNew(2);
                intVector.set(0, 10);
                intVector.set(1, 20);
                intVector.setValueCount(2);
                VarCharVector varcharVector = (VarCharVector) batch.getVector(TEST_COL2);
                varcharVector.allocateNew();
                varcharVector.set(0, "val1".getBytes(StandardCharsets.UTF_8));
                varcharVector.set(1, "val2".getBytes(StandardCharsets.UTF_8));
                varcharVector.setValueCount(2);

                ArrowVectorIterator mockIterator = Mockito.mock(ArrowVectorIterator.class);
                when(mockIterator.hasNext()).thenReturn(true, false);
                when(mockIterator.next()).thenReturn(batch);

                try (MockedStatic<JdbcToArrow> mockedJdbcToArrow = Mockito.mockStatic(JdbcToArrow.class)) {
                    mockedJdbcToArrow.when(() -> JdbcToArrow
                            .sqlToArrowVectorIterator(any(ResultSet.class), any(JdbcToArrowConfig.class)))
                            .thenReturn(mockIterator);

                    S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();
                    Split split = Split.newBuilder(s3SpillLocation, null).build();
                    Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(),
                            Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
                    ReadRecordsRequest readRecordsRequest = new ReadRecordsRequest(
                            this.federatedIdentity, TEST_CATALOG, TEST_QUERY_ID,
                            new TableName(TEST_SCHEMA_NAME, TEST_TABLE), fieldSchema, split, constraints,
                            100_000_000, 100_000_000);
                    try (BlockAllocator allocator = new BlockAllocatorImpl()) {
                        RecordResponse response = this.adbcRecordHandler.doReadRecords(allocator, readRecordsRequest);
                        assertNotNull(response);
                        assertTrue(response instanceof ReadRecordsResponse);
                        assertEquals(2, ((ReadRecordsResponse) response).getRecordCount());
                        response.close();
                    }
                }
            }
        }
    }

    @Test
    public void doReadRecordsBatchPathHandlesEmptyResult() throws Exception
    {
        ArrowVectorIterator mockIterator = Mockito.mock(ArrowVectorIterator.class);
        when(mockIterator.hasNext()).thenReturn(false);

        try (MockedStatic<JdbcToArrow> mockedJdbcToArrow = Mockito.mockStatic(JdbcToArrow.class)) {
            mockedJdbcToArrow.when(() -> JdbcToArrow
                    .sqlToArrowVectorIterator(any(ResultSet.class), any(JdbcToArrowConfig.class)))
                    .thenReturn(mockIterator);

            Schema fieldSchema = SchemaBuilder.newBuilder()
                    .addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build()).build();
            S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();
            Split split = Split.newBuilder(s3SpillLocation, null).build();
            Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(),
                    Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
            ReadRecordsRequest readRecordsRequest = new ReadRecordsRequest(
                    this.federatedIdentity, TEST_CATALOG, TEST_QUERY_ID,
                    new TableName(TEST_SCHEMA_NAME, TEST_TABLE), fieldSchema, split, constraints,
                    100_000_000, 100_000_000);
            try (BlockAllocator allocator = new BlockAllocatorImpl()) {
                RecordResponse response = this.adbcRecordHandler.doReadRecords(allocator, readRecordsRequest);
                assertNotNull(response);
                assertTrue(response instanceof ReadRecordsResponse);
                assertEquals(0, ((ReadRecordsResponse) response).getRecordCount());
                response.close();
            }
        }
    }

    @Test
    public void doReadRecordsBatchPathWithPartitionColumns() throws Exception
    {
        try (BufferAllocator testAllocator = new RootAllocator()) {
            Schema fieldSchema = SchemaBuilder.newBuilder()
                    .addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build())
                    .addField(FieldBuilder.newBuilder(TEST_PARTITION_COL, Types.MinorType.VARCHAR.getType()).build())
                    .build();
            List<Field> batchFields = Collections.singletonList(
                    Field.nullable(TEST_COL1, Types.MinorType.INT.getType()));
            try (VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(batchFields), testAllocator)) {
                batch.setRowCount(2);
                IntVector intVector = (IntVector) batch.getVector(TEST_COL1);
                intVector.allocateNew(2);
                intVector.set(0, 100);
                intVector.set(1, 200);
                intVector.setValueCount(2);

                ArrowVectorIterator mockIterator = Mockito.mock(ArrowVectorIterator.class);
                when(mockIterator.hasNext()).thenReturn(true, false);
                when(mockIterator.next()).thenReturn(batch);

                try (MockedStatic<JdbcToArrow> mockedJdbcToArrow = Mockito.mockStatic(JdbcToArrow.class)) {
                    mockedJdbcToArrow.when(() -> JdbcToArrow
                            .sqlToArrowVectorIterator(any(ResultSet.class), any(JdbcToArrowConfig.class)))
                            .thenReturn(mockIterator);

                    S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();
                    Split split = Split.newBuilder(s3SpillLocation, null)
                            .add(TEST_PARTITION_COL, TEST_PARTITION_VALUE).build();
                    Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(),
                            Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
                    ReadRecordsRequest readRecordsRequest = new ReadRecordsRequest(
                            this.federatedIdentity, TEST_CATALOG, TEST_QUERY_ID,
                            new TableName(TEST_SCHEMA_NAME, TEST_TABLE), fieldSchema, split, constraints,
                            100_000_000, 100_000_000);
                    try (BlockAllocator allocator = new BlockAllocatorImpl()) {
                        RecordResponse response = this.adbcRecordHandler.doReadRecords(allocator, readRecordsRequest);
                        assertNotNull(response);
                        assertTrue(response instanceof ReadRecordsResponse);
                        assertEquals(2, ((ReadRecordsResponse) response).getRecordCount());
                        response.close();
                    }
                }
            }
        }
    }

    @Test
    public void readWithConstraintHandlesNullValues() throws Exception
    {
        try (BufferAllocator testAllocator = new RootAllocator()) {
            Schema fieldSchema = SchemaBuilder.newBuilder()
                    .addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build())
                    .addField(FieldBuilder.newBuilder(TEST_COL2, Types.MinorType.VARCHAR.getType()).build())
                    .build();
            List<Field> batchFields = Arrays.asList(
                    Field.nullable(TEST_COL1, Types.MinorType.INT.getType()),
                    Field.nullable(TEST_COL2, Types.MinorType.VARCHAR.getType()));
            try (VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(batchFields), testAllocator)) {
                batch.setRowCount(2);
                IntVector intVector = (IntVector) batch.getVector(TEST_COL1);
                intVector.allocateNew(2);
                intVector.set(0, 1);
                intVector.setNull(1);
                intVector.setValueCount(2);
                VarCharVector varcharVector = (VarCharVector) batch.getVector(TEST_COL2);
                varcharVector.allocateNew();
                varcharVector.setNull(0);
                varcharVector.set(1, "testVal".getBytes(StandardCharsets.UTF_8));
                varcharVector.setValueCount(2);

                ArrowVectorIterator mockIterator = Mockito.mock(ArrowVectorIterator.class);
                when(mockIterator.hasNext()).thenReturn(true, false);
                when(mockIterator.next()).thenReturn(batch);

                try (MockedStatic<JdbcToArrow> mockedJdbcToArrow = Mockito.mockStatic(JdbcToArrow.class)) {
                    mockedJdbcToArrow.when(() -> JdbcToArrow
                            .sqlToArrowVectorIterator(any(ResultSet.class), any(JdbcToArrowConfig.class)))
                            .thenReturn(mockIterator);

                    try (BlockAllocator allocator = new BlockAllocatorImpl()) {
                        ConstraintEvaluator constraintEvaluator = Mockito.mock(ConstraintEvaluator.class);
                        when(constraintEvaluator.apply(nullable(String.class), any())).thenReturn(true);
                        S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();
                        SpillConfig spillConfig = Mockito.mock(SpillConfig.class);
                        when(spillConfig.getSpillLocation()).thenReturn(s3SpillLocation);
                        try (S3BlockSpiller spiller = new S3BlockSpiller(this.amazonS3, spillConfig, allocator, fieldSchema,
                                constraintEvaluator, ImmutableMap.of())) {
                            Split split = Split.newBuilder(s3SpillLocation, null).build();
                            Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(),
                                    Collections.emptyList(), Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
                            ReadRecordsRequest readRecordsRequest = new ReadRecordsRequest(
                                    this.federatedIdentity, TEST_CATALOG, TEST_QUERY_ID,
                                    new TableName(TEST_SCHEMA_NAME, TEST_TABLE), fieldSchema, split, constraints, 1024, 1024);
                            when(amazonS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                                    .thenReturn(PutObjectResponse.builder().build());
                            this.adbcRecordHandler.readWithConstraint(spiller, readRecordsRequest, queryStatusChecker);
                        }
                    }
                }
            }
        }
    }
}
