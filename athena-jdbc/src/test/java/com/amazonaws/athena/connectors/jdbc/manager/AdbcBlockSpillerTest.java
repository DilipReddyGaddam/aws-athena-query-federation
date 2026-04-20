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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

public class AdbcBlockSpillerTest
{
    @Test
    public void writeBatchTransfersDataCorrectly() throws Exception
    {
        try (BufferAllocator testAllocator = new RootAllocator()) {
            Schema fieldSchema = SchemaBuilder.newBuilder()
                    .addField(FieldBuilder.newBuilder("col1", Types.MinorType.INT.getType()).build())
                    .addField(FieldBuilder.newBuilder("col2", Types.MinorType.VARCHAR.getType()).build())
                    .build();

            List<Field> batchFields = Arrays.asList(
                    Field.nullable("col1", Types.MinorType.INT.getType()),
                    Field.nullable("col2", Types.MinorType.VARCHAR.getType()));
            try (VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(batchFields), testAllocator)) {
                batch.setRowCount(3);

                IntVector intVector = (IntVector) batch.getVector("col1");
                intVector.allocateNew(3);
                intVector.set(0, 10);
                intVector.set(1, 20);
                intVector.set(2, 30);
                intVector.setValueCount(3);

                VarCharVector varcharVector = (VarCharVector) batch.getVector("col2");
                varcharVector.allocateNew();
                varcharVector.set(0, "a".getBytes(StandardCharsets.UTF_8));
                varcharVector.set(1, "b".getBytes(StandardCharsets.UTF_8));
                varcharVector.set(2, "c".getBytes(StandardCharsets.UTF_8));
                varcharVector.setValueCount(3);

                S3Client s3 = Mockito.mock(S3Client.class);
                S3SpillLocation spillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();
                SpillConfig spillConfig = SpillConfig.newBuilder()
                        .withSpillLocation(spillLocation)
                        .withMaxBlockBytes(100_000_000)
                        .withMaxInlineBlockBytes(100_000_000)
                        .withRequestId("testQuery")
                        .build();

                try (BlockAllocator allocator = new BlockAllocatorImpl()) {
                    ConstraintEvaluator evaluator = Mockito.mock(ConstraintEvaluator.class);
                    when(evaluator.apply(nullable(String.class), any())).thenReturn(true);

                    try (AdbcBlockSpiller spiller = new AdbcBlockSpiller(s3, spillConfig, allocator,
                            fieldSchema, evaluator)) {
                        spiller.writeBatch(batch, Collections.emptyMap());

                        assertFalse(spiller.spilled());
                        Block block = spiller.getBlock();
                        assertNotNull(block);
                        assertEquals(3, block.getRowCount());
                    }
                }
            }
        }
    }

    @Test
    public void writeBatchHandlesPartitionColumns() throws Exception
    {
        try (BufferAllocator testAllocator = new RootAllocator()) {
            Schema fieldSchema = SchemaBuilder.newBuilder()
                    .addField(FieldBuilder.newBuilder("col1", Types.MinorType.INT.getType()).build())
                    .addField(FieldBuilder.newBuilder("partCol", Types.MinorType.VARCHAR.getType()).build())
                    .build();

            // Batch only has the data column, not the partition column
            List<Field> batchFields = Collections.singletonList(
                    Field.nullable("col1", Types.MinorType.INT.getType()));
            try (VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(batchFields), testAllocator)) {
                batch.setRowCount(2);

                IntVector intVector = (IntVector) batch.getVector("col1");
                intVector.allocateNew(2);
                intVector.set(0, 100);
                intVector.set(1, 200);
                intVector.setValueCount(2);

                S3Client s3 = Mockito.mock(S3Client.class);
                S3SpillLocation spillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();
                SpillConfig spillConfig = SpillConfig.newBuilder()
                        .withSpillLocation(spillLocation)
                        .withMaxBlockBytes(100_000_000)
                        .withMaxInlineBlockBytes(100_000_000)
                        .withRequestId("testQuery")
                        .build();

                try (BlockAllocator allocator = new BlockAllocatorImpl()) {
                    ConstraintEvaluator evaluator = Mockito.mock(ConstraintEvaluator.class);
                    when(evaluator.apply(nullable(String.class), any())).thenReturn(true);

                    try (AdbcBlockSpiller spiller = new AdbcBlockSpiller(s3, spillConfig, allocator,
                            fieldSchema, evaluator)) {
                        Map<String, String> partitionValues = Collections.singletonMap("partCol", "partition1");
                        spiller.writeBatch(batch, partitionValues);

                        assertFalse(spiller.spilled());
                        Block block = spiller.getBlock();
                        assertEquals(2, block.getRowCount());
                    }
                }
            }
        }
    }

    @Test
    public void writeBatchAccumulatesMultipleBatches() throws Exception
    {
        try (BufferAllocator testAllocator = new RootAllocator()) {
            Schema fieldSchema = SchemaBuilder.newBuilder()
                    .addField(FieldBuilder.newBuilder("col1", Types.MinorType.INT.getType()).build())
                    .build();

            List<Field> batchFields = Collections.singletonList(
                    Field.nullable("col1", Types.MinorType.INT.getType()));

            S3Client s3 = Mockito.mock(S3Client.class);
            S3SpillLocation spillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();
            SpillConfig spillConfig = SpillConfig.newBuilder()
                    .withSpillLocation(spillLocation)
                    .withMaxBlockBytes(100_000_000)
                    .withMaxInlineBlockBytes(100_000_000)
                    .withRequestId("testQuery")
                    .build();

            try (BlockAllocator allocator = new BlockAllocatorImpl()) {
                ConstraintEvaluator evaluator = Mockito.mock(ConstraintEvaluator.class);
                when(evaluator.apply(nullable(String.class), any())).thenReturn(true);

                try (AdbcBlockSpiller spiller = new AdbcBlockSpiller(s3, spillConfig, allocator,
                        fieldSchema, evaluator)) {
                    // Write first batch
                    try (VectorSchemaRoot batch1 = VectorSchemaRoot.create(new Schema(batchFields), testAllocator)) {
                        batch1.setRowCount(2);
                        IntVector v1 = (IntVector) batch1.getVector("col1");
                        v1.allocateNew(2);
                        v1.set(0, 1);
                        v1.set(1, 2);
                        v1.setValueCount(2);
                        spiller.writeBatch(batch1, Collections.emptyMap());
                    }

                    // Write second batch
                    try (VectorSchemaRoot batch2 = VectorSchemaRoot.create(new Schema(batchFields), testAllocator)) {
                        batch2.setRowCount(3);
                        IntVector v2 = (IntVector) batch2.getVector("col1");
                        v2.allocateNew(3);
                        v2.set(0, 3);
                        v2.set(1, 4);
                        v2.set(2, 5);
                        v2.setValueCount(3);
                        spiller.writeBatch(batch2, Collections.emptyMap());
                    }

                    // Should accumulate: 2 + 3 = 5 rows
                    assertFalse(spiller.spilled());
                    assertEquals(5, spiller.getBlock().getRowCount());
                }
            }
        }
    }

    @Test
    public void writeBatchSpillsWhenBlockExceedsMaxSize() throws Exception
    {
        try (BufferAllocator testAllocator = new RootAllocator()) {
            Schema fieldSchema = SchemaBuilder.newBuilder()
                    .addField(FieldBuilder.newBuilder("col1", Types.MinorType.INT.getType()).build())
                    .build();

            List<Field> batchFields = Collections.singletonList(
                    Field.nullable("col1", Types.MinorType.INT.getType()));

            S3Client s3 = Mockito.mock(S3Client.class);
            when(s3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                    .thenReturn(PutObjectResponse.builder().build());

            S3SpillLocation spillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();
            // Very small max block size to force spill
            SpillConfig spillConfig = SpillConfig.newBuilder()
                    .withSpillLocation(spillLocation)
                    .withMaxBlockBytes(1)
                    .withMaxInlineBlockBytes(1)
                    .withRequestId("testQuery")
                    .build();

            try (BlockAllocator allocator = new BlockAllocatorImpl()) {
                ConstraintEvaluator evaluator = Mockito.mock(ConstraintEvaluator.class);
                when(evaluator.apply(nullable(String.class), any())).thenReturn(true);

                try (AdbcBlockSpiller spiller = new AdbcBlockSpiller(s3, spillConfig, allocator,
                        fieldSchema, evaluator)) {
                    try (VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(batchFields), testAllocator)) {
                        batch.setRowCount(10);
                        IntVector v = (IntVector) batch.getVector("col1");
                        v.allocateNew(10);
                        for (int i = 0; i < 10; i++) {
                            v.set(i, i);
                        }
                        v.setValueCount(10);
                        spiller.writeBatch(batch, Collections.emptyMap());
                    }

                    assertTrue(spiller.spilled());
                    assertFalse(spiller.getSpillLocations().isEmpty());
                }
            }
        }
    }

    @Test
    public void writeBatchHandlesEmptyBatch() throws Exception
    {
        try (BufferAllocator testAllocator = new RootAllocator()) {
            Schema fieldSchema = SchemaBuilder.newBuilder()
                    .addField(FieldBuilder.newBuilder("col1", Types.MinorType.INT.getType()).build())
                    .build();

            List<Field> batchFields = Collections.singletonList(
                    Field.nullable("col1", Types.MinorType.INT.getType()));
            try (VectorSchemaRoot batch = VectorSchemaRoot.create(new Schema(batchFields), testAllocator)) {
                batch.setRowCount(0);

                S3Client s3 = Mockito.mock(S3Client.class);
                S3SpillLocation spillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();
                SpillConfig spillConfig = SpillConfig.newBuilder()
                        .withSpillLocation(spillLocation)
                        .withMaxBlockBytes(100_000_000)
                        .withMaxInlineBlockBytes(100_000_000)
                        .withRequestId("testQuery")
                        .build();

                try (BlockAllocator allocator = new BlockAllocatorImpl()) {
                    ConstraintEvaluator evaluator = Mockito.mock(ConstraintEvaluator.class);

                    try (AdbcBlockSpiller spiller = new AdbcBlockSpiller(s3, spillConfig, allocator,
                            fieldSchema, evaluator)) {
                        spiller.writeBatch(batch, Collections.emptyMap());

                        assertFalse(spiller.spilled());
                        assertEquals(0, spiller.getBlock().getRowCount());
                    }
                }
            }
        }
    }
}
