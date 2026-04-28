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
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.security.AesGcmBlockCrypto;
import com.amazonaws.athena.connector.lambda.security.BlockCrypto;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.NoOpBlockCrypto;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

/**
 * BlockSpiller implementation optimized for ADBC batch-level data transfer.
 *
 * <p>Instead of the row-by-row {@link BlockWriter.RowWriter} path used by
 * {@link com.amazonaws.athena.connector.lambda.data.S3BlockSpiller},
 * this spiller provides a {@link #writeBatch(VectorSchemaRoot, Map)} method that
 * copies data column-by-column using Arrow's {@code copyFromSafe()} for direct
 * buffer-to-buffer transfers. This eliminates:</p>
 * <ul>
 *   <li>Object boxing/unboxing from {@code FieldVector.getObject()}</li>
 *   <li>Per-value type dispatch in {@code BlockUtils.setValue()}</li>
 *   <li>Per-value constraint evaluation</li>
 * </ul>
 *
 * <p>Handles block size management and S3 spilling with optional encryption,
 * matching the behavior expected by Athena's read response protocol.</p>
 */
public class AdbcBlockSpiller
        implements BlockSpiller, AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AdbcBlockSpiller.class);

    private final S3Client amazonS3;
    private final BlockAllocator allocator;
    private final SpillConfig spillConfig;
    private final Schema schema;
    private final BlockCrypto blockCrypto;
    private final ConstraintEvaluator constraintEvaluator;

    private Block currentBlock;
    private final List<SpillLocation> spillLocations = new ArrayList<>();
    private final AtomicLong spillNumber = new AtomicLong(0);
    private final AtomicLong totalBytesSpilled = new AtomicLong(0);
    private final long startTime = System.currentTimeMillis();

    public AdbcBlockSpiller(
            S3Client amazonS3,
            SpillConfig spillConfig,
            BlockAllocator allocator,
            Schema schema,
            ConstraintEvaluator constraintEvaluator)
    {
        this.amazonS3 = requireNonNull(amazonS3, "amazonS3 was null");
        this.spillConfig = requireNonNull(spillConfig, "spillConfig was null");
        this.allocator = requireNonNull(allocator, "allocator was null");
        this.schema = requireNonNull(schema, "schema was null");
        this.constraintEvaluator = constraintEvaluator;
        this.blockCrypto = (spillConfig.getEncryptionKey() != null)
                ? new AesGcmBlockCrypto(allocator)
                : new NoOpBlockCrypto(allocator);
    }

    /**
     * Writes an ADBC Arrow batch to the spiller using column-level vector copies.
     *
     * <p>For each field in the expected schema:</p>
     * <ul>
     *   <li>Partition columns are filled with their constant split values</li>
     *   <li>Data columns are copied from the ADBC batch using {@code copyFromSafe()},
     *       which performs direct buffer-to-buffer transfers without Object boxing</li>
     * </ul>
     *
     * @param batch The ADBC VectorSchemaRoot batch containing query results
     * @param partitionValues The partition column values from the split properties
     */
    public void writeBatch(VectorSchemaRoot batch, Map<String, String> partitionValues)
    {
        int batchRows = batch.getRowCount();
        if (batchRows == 0) {
            return;
        }

        ensureBlock();
        int blockOffset = currentBlock.getRowCount();

        for (Field field : schema.getFields()) {
            String fieldName = field.getName();
            FieldVector targetVector = currentBlock.getFieldVector(fieldName);

            if (targetVector == null) {
                continue;
            }

            if (partitionValues.containsKey(fieldName)) {
                // Partition column: fill with constant value
                String partValue = partitionValues.get(fieldName);
                for (int i = 0; i < batchRows; i++) {
                    BlockUtils.setValue(targetVector, blockOffset + i, partValue);
                }
            }
            else {
                FieldVector sourceVector = batch.getVector(fieldName);
                if (sourceVector != null) {
                    if (sourceVector.getMinorType() == targetVector.getMinorType()) {
                        // Fast path: same Arrow type — direct buffer-to-buffer copy
                        // No Object boxing, no type dispatch in BlockUtils.setValue
                        for (int i = 0; i < batchRows; i++) {
                            targetVector.copyFromSafe(i, blockOffset + i, sourceVector);
                        }
                    }
                    else {
                        // Safe path: types differ (e.g., native driver returns Decimal256 for numeric,
                        // or text for arrays/uuid). Fall back to getObject() + type-specific handling.
                        LOGGER.debug("Type mismatch for field '{}': source={}, target={}. Using coercion path.",
                                fieldName, sourceVector.getMinorType(), targetVector.getMinorType());
                        boolean targetIsList = field.getType().getTypeID() == ArrowType.ArrowTypeID.List;
                        boolean targetIsDecimal = targetVector instanceof DecimalVector;
                        for (int i = 0; i < batchRows; i++) {
                            Object value = sourceVector.isNull(i) ? null : sourceVector.getObject(i);
                            if (value == null) {
                                BlockUtils.setValue(targetVector, blockOffset + i, null);
                            }
                            else if (targetIsDecimal) {
                                // Native ADBC driver returns Decimal256 for PostgreSQL numeric;
                                // target expects Decimal128. Convert via BigDecimal with correct scale.
                                DecimalVector dv = (DecimalVector) targetVector;
                                BigDecimal bd = (value instanceof BigDecimal)
                                        ? (BigDecimal) value
                                        : new BigDecimal(value.toString());
                                dv.setSafe(blockOffset + i, bd.setScale(dv.getScale(), RoundingMode.HALF_UP));
                            }
                            else if (targetIsList && value instanceof org.apache.arrow.vector.util.Text) {
                                value = parsePostgresArray(((org.apache.arrow.vector.util.Text) value).toString());
                                BlockUtils.setComplexValue(targetVector, blockOffset + i,
                                        FieldResolver.DEFAULT, value);
                            }
                            else if (targetIsList) {
                                BlockUtils.setComplexValue(targetVector, blockOffset + i,
                                        FieldResolver.DEFAULT, value);
                            }
                            else {
                                BlockUtils.setValue(targetVector, blockOffset + i, value);
                            }
                        }
                    }
                }
            }
        }

        currentBlock.setRowCount(blockOffset + batchRows);

        if (currentBlock.getSize() > spillConfig.getMaxBlockBytes()) {
            LOGGER.info("writeBatch: Spilling block with {} rows and {} bytes",
                    currentBlock.getRowCount(), currentBlock.getSize());
            doSpillBlock(currentBlock);
            currentBlock = null;
        }
    }

    /**
     * Fallback row-by-row write path for compatibility with the BlockWriter interface.
     */
    @Override
    public void writeRows(BlockWriter.RowWriter rowWriter)
    {
        ensureBlock();
        int rowCount = currentBlock.getRowCount();

        int rows;
        try {
            rows = rowWriter.writeRows(currentBlock, rowCount);
        }
        catch (Exception ex) {
            throw (ex instanceof RuntimeException) ? (RuntimeException) ex : new RuntimeException(ex);
        }

        if (rows > 0) {
            currentBlock.setRowCount(rowCount + rows);
        }

        if (currentBlock.getSize() > spillConfig.getMaxBlockBytes()) {
            doSpillBlock(currentBlock);
            currentBlock = null;
        }
    }

    @Override
    public boolean spilled()
    {
        ensureBlock();
        return !spillLocations.isEmpty() || currentBlock.getSize() >= spillConfig.getMaxInlineBlockSize();
    }

    @Override
    public Block getBlock()
    {
        return currentBlock;
    }

    @Override
    public List<SpillLocation> getSpillLocations()
    {
        // Flush the in-progress block if it has data
        if (currentBlock != null && currentBlock.getRowCount() > 0) {
            LOGGER.info("getSpillLocations: Spilling final block with {} rows and {} bytes",
                    currentBlock.getRowCount(), currentBlock.getSize());
            doSpillBlock(currentBlock);
            currentBlock = null;
            ensureBlock();
        }
        return spillLocations;
    }

    @Override
    public ConstraintEvaluator getConstraintEvaluator()
    {
        return constraintEvaluator;
    }

    @Override
    public void close()
    {
        LOGGER.info("close: Spilled a total of {} bytes in {} ms",
                totalBytesSpilled.get(), System.currentTimeMillis() - startTime);
    }

    private void ensureBlock()
    {
        if (currentBlock == null) {
            currentBlock = allocator.createBlock(schema);
        }
    }

    private void doSpillBlock(Block block)
    {
        S3SpillLocation spillLocation = makeSpillLocation();
        EncryptionKey encryptionKey = spillConfig.getEncryptionKey();

        byte[] bytes = blockCrypto.encrypt(encryptionKey, block);
        totalBytesSpilled.addAndGet(bytes.length);

        LOGGER.info("doSpillBlock: Spilling block of {} bytes to {}", bytes.length, spillLocation);

        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(spillLocation.getBucket())
                .key(spillLocation.getKey())
                .contentLength((long) bytes.length)
                .build();
        amazonS3.putObject(request, RequestBody.fromBytes(bytes));

        spillLocations.add(spillLocation);

        try {
            block.close();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to close spilled block", e);
        }
    }

    /**
     * Creates monotonically increasing spill locations matching the convention
     * expected by Athena's read engine for pipelined reads.
     */
    private S3SpillLocation makeSpillLocation()
    {
        S3SpillLocation splitSpillLocation = (S3SpillLocation) spillConfig.getSpillLocation();
        String blockKey = splitSpillLocation.getKey() + "." + spillNumber.getAndIncrement();
        return new S3SpillLocation(splitSpillLocation.getBucket(), blockKey, false);
    }

    /**
     * Parses a PostgreSQL array text representation (e.g., "{food,bar,baz}") into a List of strings.
     */
    private static List<String> parsePostgresArray(String pgArray)
    {
        List<String> result = new ArrayList<>();
        if (pgArray == null || pgArray.equals("{}") || pgArray.equals("{NULL}")) {
            return result;
        }
        // Strip outer braces
        String inner = pgArray.substring(1, pgArray.length() - 1);
        if (!inner.isEmpty()) {
            for (String element : inner.split(",")) {
                result.add(element.trim());
            }
        }
        return result;
    }
}
