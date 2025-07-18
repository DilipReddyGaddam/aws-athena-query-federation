/*-
 * #%L
 * athena-timestream
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.timestream;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient;
import software.amazon.awssdk.services.timestreamquery.model.QueryRequest;
import software.amazon.awssdk.services.timestreamquery.model.QueryResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.VIEW_METADATA_FIELD;
import static com.amazonaws.athena.connectors.timestream.TestUtils.makeMockQueryResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TimestreamRecordHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(TimestreamRecordHandlerTest.class);

    private static final FederatedIdentity IDENTITY = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());

    private TimestreamRecordHandler handler;
    private BlockAllocator allocator;
    private List<ByteHolder> mockS3Storage = new ArrayList<>();
    private S3Client amazonS3;
    private S3BlockSpillReader spillReader;
    private Schema schemaForRead;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private static final String DEFAULT_CATALOG = "my_catalog";
    private static final String DEFAULT_SCHEMA = "my_schema";
    private static final String TEST_TABLE = "my_table";
    private static final String TEST_VIEW = "my_view";

    @Rule
    public TestName testName = new TestName();

    @Mock
    private TimestreamQueryClient mockClient;

    @Mock
    private SecretsManagerClient mockSecretsManager;

    @Mock
    private AthenaClient mockAthena;

    private class ByteHolder
    {
        private byte[] bytes;

        public void setBytes(byte[] bytes)
        {
            this.bytes = bytes;
        }

        public byte[] getBytes()
        {
            return bytes;
        }
    }

    @Before
    public void setUp()
            throws IOException
    {
        logger.info("{}: enter", testName.getMethodName());

        allocator = new BlockAllocatorImpl();

        amazonS3 = mock(S3Client.class);

        when(amazonS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    InputStream inputStream = ((RequestBody) invocationOnMock.getArguments()[1]).contentStreamProvider().newStream();
                    ByteHolder byteHolder = new ByteHolder();
                    byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                    synchronized (mockS3Storage) {
                        mockS3Storage.add(byteHolder);
                        logger.info("puObject: total size " + mockS3Storage.size());
                    }
                    return PutObjectResponse.builder().build();
                });

        when(amazonS3.getObject(any(GetObjectRequest.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    ByteHolder byteHolder;
                    synchronized (mockS3Storage) {
                        byteHolder = mockS3Storage.get(0);
                        mockS3Storage.remove(0);
                        logger.info("getObject: total size " + mockS3Storage.size());
                    }
                    return new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream(byteHolder.getBytes()));
                });

        schemaForRead = SchemaBuilder.newBuilder()
                .addField("measure_name", Types.MinorType.VARCHAR.getType())
                .addField("measure_value::double", Types.MinorType.FLOAT8.getType())
                .addField("az", Types.MinorType.VARCHAR.getType())
                .addField("time", Types.MinorType.DATEMILLI.getType())
                .addField("hostname", Types.MinorType.VARCHAR.getType())
                .addField("region", Types.MinorType.VARCHAR.getType())
                .build();

        handler = new TimestreamRecordHandler(amazonS3, mockSecretsManager, mockAthena, mockClient, com.google.common.collect.ImmutableMap.of());
        spillReader = new S3BlockSpillReader(amazonS3, allocator);
    }

    @After
    public void after()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void doReadRecordsNoSpill()
            throws Exception
    {
        int numRowsGenerated = 1_000;
        String expectedQuery = "SELECT measure_name, measure_value::double, az, time, hostname, region FROM \"my_schema\".\"my_table\" WHERE (\"az\" IN ('us-east-1a','us-east-1b'))";

        QueryResponse mockResult = makeMockQueryResult(schemaForRead, numRowsGenerated);
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            QueryRequest request = (QueryRequest) invocationOnMock.getArguments()[0];
                            assertEquals(expectedQuery, request.queryString().replace("\n", ""));
                            return mockResult;
                        }
                );

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("az", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add("us-east-1a")
                .add("us-east-1b").build());

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create());

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schemaForRead,
                splitBuilder.build(),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());

        assertTrue(response.getRecords().getRowCount() > 0);

        //ensure we actually filtered something out
        assertTrue(response.getRecords().getRowCount() < numRowsGenerated);

        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecordsSpill()
            throws Exception
    {
        String expectedQuery = "SELECT measure_name, measure_value::double, az, time, hostname, region FROM \"my_schema\".\"my_table\" WHERE (\"az\" IN ('us-east-1a','us-east-1b'))";

        QueryResponse mockResult = makeMockQueryResult(schemaForRead, 100_000);
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            QueryRequest request = (QueryRequest) invocationOnMock.getArguments()[0];
                            assertEquals(expectedQuery, request.queryString().replace("\n", ""));
                            return mockResult;
                        }
                );

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("az", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add("us-east-1a")
                .add("us-east-1b").build());

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create());

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schemaForRead,
                splitBuilder.build(),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                1_500_000L, //~1.5MB so we should see some spill
                0L
        );
        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof RemoteReadRecordsResponse);

        try (RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) rawResponse) {
            logger.info("doReadRecordsSpill: remoteBlocks[{}]", response.getRemoteBlocks().size());

            assertTrue(response.getNumberBlocks() > 1);

            int blockNum = 0;
            for (SpillLocation next : response.getRemoteBlocks()) {
                S3SpillLocation spillLocation = (S3SpillLocation) next;
                try (Block block = spillReader.read(spillLocation, response.getEncryptionKey(), response.getSchema())) {

                    logger.info("doReadRecordsSpill: blockNum[{}] and recordCount[{}]", blockNum++, block.getRowCount());
                    // assertTrue(++blockNum < response.getRemoteBlocks().size() && block.getRowCount() > 10_000);

                    logger.info("doReadRecordsSpill: {}", BlockUtils.rowToString(block, 0));
                    assertNotNull(BlockUtils.rowToString(block, 0));
                }
            }
        }
    }

    @Test
    public void readRecordsView()
            throws Exception
    {
        logger.info("readRecordsView - enter");

        Schema schemaForReadView = SchemaBuilder.newBuilder()
                .addField("measure_name", Types.MinorType.VARCHAR.getType())
                .addField("az", Types.MinorType.VARCHAR.getType())
                .addField("value", Types.MinorType.FLOAT8.getType())
                .addField("num_samples", Types.MinorType.BIGINT.getType())
                .addMetadata(VIEW_METADATA_FIELD, "select measure_name, az,sum(\"measure_value::double\") as value, count(*) as num_samples from \"" +
                        DEFAULT_SCHEMA + "\".\"" + TEST_TABLE + "\" group by measure_name, az")
                .build();

        String expectedQuery = "WITH t1 AS ( select measure_name, az,sum(\"measure_value::double\") as value, count(*) as num_samples from \"my_schema\".\"my_table\" group by measure_name, az )  SELECT measure_name, az, value, num_samples FROM t1 WHERE (\"az\" IN ('us-east-1a','us-east-1b'))";

        QueryResponse mockResult = makeMockQueryResult(schemaForReadView, 1_000);
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            QueryRequest request = (QueryRequest) invocationOnMock.getArguments()[0];
                            assertEquals(expectedQuery, request.queryString().replace("\n", ""));
                            return mockResult;
                        }
                );

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split split = Split.newBuilder(splitLoc, null).build();

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("az", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add("us-east-1a")
                .add("us-east-1b").build());

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                "default",
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_VIEW),
                schemaForReadView,
                split,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("readRecordsView: rows[{}]", response.getRecordCount());

        for (int i = 0; i < response.getRecordCount() && i < 10; i++) {
            logger.info("readRecordsView: {}", BlockUtils.rowToString(response.getRecords(), i));
        }

        logger.info("readRecordsView - exit");
    }

    @Test
    public void readRecordsTimeSeriesView()
            throws Exception
    {
        logger.info("readRecordsTimeSeriesView - enter");

        Schema schemaForReadView = SchemaBuilder.newBuilder()
                .addField("region", Types.MinorType.VARCHAR.getType())
                .addField("az", Types.MinorType.VARCHAR.getType())
                .addField("hostname", Types.MinorType.VARCHAR.getType())
                .addField(FieldBuilder.newBuilder("cpu_utilization", Types.MinorType.LIST.getType())
                        .addField(FieldBuilder.newBuilder("cpu_utilization", Types.MinorType.STRUCT.getType())
                                .addDateMilliField("time")
                                .addFloat8Field("measure_value::double")
                                .build())
                        .build())
                .addMetadata(VIEW_METADATA_FIELD, "select az, hostname, region,  CREATE_TIME_SERIES(time, measure_value::double) as cpu_utilization from \"" + DEFAULT_SCHEMA + "\".\"" + TEST_TABLE + "\" WHERE measure_name = 'cpu_utilization' GROUP BY measure_name, az, hostname, region")
                .build();

        String expectedQuery = "WITH t1 AS ( select az, hostname, region,  CREATE_TIME_SERIES(time, measure_value::double) as cpu_utilization from \"my_schema\".\"my_table\" WHERE measure_name = 'cpu_utilization' GROUP BY measure_name, az, hostname, region )  SELECT region, az, hostname, cpu_utilization FROM t1 WHERE (\"az\" IN ('us-east-1a','us-east-1b'))";

        QueryResponse mockResult = makeMockQueryResult(schemaForReadView, 1_000);
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            QueryRequest request = (QueryRequest) invocationOnMock.getArguments()[0];
                            assertEquals("actual: " + request.queryString(), expectedQuery, request.queryString().replace("\n", ""));
                            return mockResult;
                        }
                );

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split split = Split.newBuilder(splitLoc, null)
                .build();

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("az", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add("us-east-1a")
                .add("us-east-1b").build());

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                "default",
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schemaForReadView,
                split,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("readRecordsTimeSeriesView: rows[{}]", response.getRecordCount());

        for (int i = 0; i < response.getRecordCount() && i < 10; i++) {
            logger.info("readRecordsTimeSeriesView: {}", BlockUtils.rowToString(response.getRecords(), i));
        }

        logger.info("readRecordsTimeSeriesView - exit");
    }

    @Test
    public void doReadRecordsNoSpillValidateTimeStamp()
            throws Exception
    {

        int numRows = 10;
        String expectedQuery = "SELECT measure_name, measure_value::double, az, time, hostname, region FROM \"my_schema\".\"my_table\" WHERE (\"az\" IN ('us-east-1a'))";

        QueryResponse mockResult = makeMockQueryResult(schemaForRead, numRows, numRows, false);
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            QueryRequest request = (QueryRequest) invocationOnMock.getArguments()[0];
                            assertEquals(expectedQuery, request.queryString().replace("\n", ""));
                            return mockResult;
                        }
                );

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("az", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add("us-east-1a").build());

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create());

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schemaForRead,
                splitBuilder.build(),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());

        assertTrue(response.getRecords().getRowCount() > 0);

        Block block = response.getRecords();
        FieldReader time = block.getFieldReader("time");
        for (int i = 0; i < response.getRecordCount() && i < numRows; i++) {
            time.setPosition(i);
            assertTrue(time.readObject() instanceof LocalDateTime);
            assertEquals(TestUtils.startDate.plusDays(i).truncatedTo(ChronoUnit.MILLIS), time.readObject());
        }

        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }
}
