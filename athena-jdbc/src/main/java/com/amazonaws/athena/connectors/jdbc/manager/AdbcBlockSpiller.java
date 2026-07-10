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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class AdbcBlockSpiller
implements BlockSpiller,
AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AdbcBlockSpiller.class);
    private final S3Client amazonS3;
    private final BlockAllocator allocator;
    private final SpillConfig spillConfig;
    private final Schema schema;
    private final BlockCrypto blockCrypto;
    private final ConstraintEvaluator constraintEvaluator;
    private Block currentBlock;
    private final List<SpillLocation> spillLocations = new CopyOnWriteArrayList<SpillLocation>();
    private final AtomicLong spillNumber = new AtomicLong(0L);
    private final AtomicLong totalBytesSpilled = new AtomicLong(0L);
    private final AtomicLong totalRowsSpilled = new AtomicLong(0L);
    private final long startTime = System.currentTimeMillis();
    private long accumulatedBytes;
    private final ThreadPoolExecutor asyncSpillPool;
    private final ReadWriteLock spillLock = new StampedLock().asReadWriteLock();
    private final AtomicReference<RuntimeException> asyncException = new AtomicReference<RuntimeException>(null);

    public AdbcBlockSpiller(S3Client amazonS3, SpillConfig spillConfig, BlockAllocator allocator, Schema schema, ConstraintEvaluator constraintEvaluator) {
        this.amazonS3 = Objects.requireNonNull(amazonS3, "amazonS3 was null");
        this.spillConfig = Objects.requireNonNull(spillConfig, "spillConfig was null");
        this.allocator = Objects.requireNonNull(allocator, "allocator was null");
        this.schema = Objects.requireNonNull(schema, "schema was null");
        this.constraintEvaluator = constraintEvaluator;
        this.blockCrypto = spillConfig.getEncryptionKey() != null ? new AesGcmBlockCrypto(allocator) : new NoOpBlockCrypto(allocator);
        int numThreads = Math.max(1, spillConfig.getNumSpillThreads());
        this.asyncSpillPool = new ThreadPoolExecutor(numThreads, numThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(numThreads * 2), (r, executor) -> {
            if (!executor.isShutdown()) {
                try {
                    executor.getQueue().put(r);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RejectedExecutionException("Interrupted while queuing spill task", e);
                }
            }
        });
    }

    public void writeBatch(VectorSchemaRoot batch, Map<String, String> partitionValues) {
        int batchRows = batch.getRowCount();
        if (batchRows == 0) {
            return;
        }
        this.ensureBlock();
        if (this.currentBlock.getRowCount() > 0 && this.accumulatedBytes >= this.spillConfig.getMaxBlockBytes()) {
            this.doSpillBlock(this.currentBlock);
            this.currentBlock = null;
            this.accumulatedBytes = 0L;
            this.ensureBlock();
        }
        int offset = this.currentBlock.getRowCount();
        for (Field field : this.schema.getFields()) {
            int i;
            String fieldName = field.getName();
            FieldVector targetVector = this.currentBlock.getFieldVector(fieldName);
            if (targetVector == null) continue;
            if (partitionValues.containsKey(fieldName)) {
                String partValue = partitionValues.get(fieldName);
                for (i = 0; i < batchRows; ++i) {
                    BlockUtils.setValue(targetVector, offset + i, partValue);
                }
                continue;
            }
            FieldVector sourceVector = batch.getVector(fieldName);
            if (sourceVector == null) continue;
            if (sourceVector.getMinorType() == targetVector.getMinorType()) {
                if (sourceVector instanceof BaseFixedWidthVector && targetVector instanceof BaseFixedWidthVector) {
                    BaseFixedWidthVector src = (BaseFixedWidthVector)sourceVector;
                    BaseFixedWidthVector tgt = (BaseFixedWidthVector)targetVector;
                    int typeWidth = src.getTypeWidth();
                    while (tgt.getValueCapacity() < offset + batchRows) {
                        tgt.reAlloc();
                    }
                    tgt.getDataBuffer().setBytes((long)offset * (long)typeWidth, src.getDataBuffer(), 0L, (long)batchRows * (long)typeWidth);
                    for (int i2 = 0; i2 < batchRows; ++i2) {
                        BitVectorHelper.setValidityBit(tgt.getValidityBuffer(), offset + i2, sourceVector.isNull(i2) ? 0 : 1);
                    }
                    continue;
                }
                for (i = 0; i < batchRows; ++i) {
                    targetVector.copyFromSafe(i, offset + i, sourceVector);
                }
                continue;
            }
            LOGGER.debug("Type mismatch for field '{}': source={}, target={}. Using coercion path.", new Object[]{fieldName, sourceVector.getMinorType(), targetVector.getMinorType()});
            boolean targetIsList = field.getType().getTypeID() == ArrowType.ArrowTypeID.List;
            boolean targetIsDecimal = targetVector instanceof DecimalVector;
            for (int i3 = 0; i3 < batchRows; ++i3) {
                List<String> value = sourceVector.isNull(i3) ? null : (List<String>) sourceVector.getObject(i3);
                if (value == null) {
                    BlockUtils.setValue(targetVector, offset + i3, null);
                    continue;
                }
                if (targetIsDecimal) {
                    DecimalVector dv = (DecimalVector)targetVector;
                    BigDecimal bd = value instanceof BigDecimal ? (BigDecimal)((Object)value) : new BigDecimal(value.toString());
                    dv.setSafe(offset + i3, bd.setScale(dv.getScale(), RoundingMode.HALF_UP));
                    continue;
                }
                if (targetIsList && value instanceof Text) {
                    value = AdbcBlockSpiller.parsePostgresArray(((Text)((Object)value)).toString());
                    BlockUtils.setComplexValue(targetVector, offset + i3, FieldResolver.DEFAULT, value);
                    continue;
                }
                if (targetIsList) {
                    BlockUtils.setComplexValue(targetVector, offset + i3, FieldResolver.DEFAULT, value);
                    continue;
                }
                BlockUtils.setValue(targetVector, offset + i3, value);
            }
        }
        for (Field field : this.schema.getFields()) {
            FieldVector v = this.currentBlock.getFieldVector(field.getName());
            if (v == null) continue;
            v.setValueCount(offset + batchRows);
        }
        this.currentBlock.setRowCount(offset + batchRows);
        long batchBytes = 0L;
        for (Field field : this.schema.getFields()) {
            FieldVector sv = batch.getVector(field.getName());
            if (sv == null) continue;
            batchBytes += (long)sv.getBufferSize();
        }
        this.accumulatedBytes += batchBytes;
    }

    @Override
    public void writeRows(BlockWriter.RowWriter rowWriter) {
        int rows;
        this.ensureBlock();
        int rowCount = this.currentBlock.getRowCount();
        try {
            rows = rowWriter.writeRows(this.currentBlock, rowCount);
        }
        catch (Exception ex) {
            throw ex instanceof RuntimeException ? (RuntimeException)ex : new RuntimeException(ex);
        }
        if (rows > 0) {
            this.currentBlock.setRowCount(rowCount + rows);
        }
        if (this.currentBlock.getSize() > this.spillConfig.getMaxBlockBytes()) {
            this.doSpillBlock(this.currentBlock);
            this.currentBlock = null;
        }
    }

    @Override
    public boolean spilled() {
        this.ensureBlock();
        return !this.spillLocations.isEmpty() || this.currentBlock.getSize() >= this.spillConfig.getMaxInlineBlockSize();
    }

    @Override
    public Block getBlock() {
        return this.currentBlock;
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    @Override
    public List<SpillLocation> getSpillLocations() {
        if (this.currentBlock != null && this.currentBlock.getRowCount() > 0) {
            LOGGER.info("getSpillLocations: Spilling final block with {} rows and {} bytes", (Object)this.currentBlock.getRowCount(), (Object)this.currentBlock.getSize());
            this.doSpillBlock(this.currentBlock);
            this.currentBlock = null;
            this.ensureBlock();
        }
        Lock lock = this.spillLock.writeLock();
        lock.lock();
        try {
            RuntimeException ex = this.asyncException.get();
            if (ex != null) {
                throw ex;
            }
            List<SpillLocation> list = this.spillLocations;
            return list;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public ConstraintEvaluator getConstraintEvaluator() {
        return this.constraintEvaluator;
    }

    @Override
    public void close() {
        this.asyncSpillPool.shutdown();
        try {
            if (!this.asyncSpillPool.awaitTermination(10000L, TimeUnit.MILLISECONDS)) {
                this.asyncSpillPool.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            this.asyncSpillPool.shutdownNow();
        }
        LOGGER.info("close: Spilled a total of {} bytes, {} rows in {} ms", this.totalBytesSpilled.get(), this.totalRowsSpilled.get(), System.currentTimeMillis() - this.startTime);
    }

    private void ensureBlock() {
        if (this.currentBlock == null) {
            this.currentBlock = this.allocator.createBlock(this.schema);
            this.accumulatedBytes = 0L;
            for (Field field : this.schema.getFields()) {
                FieldVector v = this.currentBlock.getFieldVector(field.getName());
                if (v == null) continue;
                v.reset();
            }
        }
    }

    private void doSpillBlock(Block block) {
        RuntimeException ex = this.asyncException.get();
        if (ex != null) {
            throw ex;
        }
        S3SpillLocation spillLocation = this.makeSpillLocation();
        EncryptionKey encryptionKey = this.spillConfig.getEncryptionKey();
        byte[] bytes = this.blockCrypto.encrypt(encryptionKey, block);
        this.totalBytesSpilled.addAndGet(bytes.length);
        this.totalRowsSpilled.addAndGet(block.getRowCount());
        LOGGER.info("doSpillBlock: Spilling block of {} bytes ({} rows) to {}", bytes.length, block.getRowCount(), spillLocation);
        Lock lock = this.spillLock.readLock();
        try {
            lock.lock();
            this.asyncSpillPool.submit(() -> {
                try {
                    PutObjectRequest request = (PutObjectRequest)PutObjectRequest.builder().bucket(spillLocation.getBucket()).key(spillLocation.getKey()).contentLength(Long.valueOf(bytes.length)).build();
                    this.amazonS3.putObject(request, RequestBody.fromBytes(bytes));
                    this.spillLocations.add(spillLocation);
                }
                catch (RuntimeException e) {
                    this.asyncException.compareAndSet(null, e);
                    LOGGER.error("Async spill failed for {}", (Object)spillLocation, (Object)e);
                }
                finally {
                    lock.unlock();
                }
            });
        }
        catch (Exception e) {
            lock.unlock();
            throw new RuntimeException("Failed to submit spill task", e);
        }
        try {
            block.close();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to close spilled block", e);
        }
    }

    private void waitForPendingSpill() {
    }

    private S3SpillLocation makeSpillLocation() {
        S3SpillLocation splitSpillLocation = (S3SpillLocation)this.spillConfig.getSpillLocation();
        String blockKey = splitSpillLocation.getKey() + "." + this.spillNumber.getAndIncrement();
        return new S3SpillLocation(splitSpillLocation.getBucket(), blockKey, false);
    }

    static List<String> parsePostgresArray(String pgArray) {
        ArrayList<String> result = new ArrayList<String>();
        if (pgArray == null || pgArray.equals("{}") || pgArray.equals("{NULL}")) {
            return result;
        }
        String inner = pgArray.substring(1, pgArray.length() - 1);
        if (!inner.isEmpty()) {
            for (String element : inner.split(",")) {
                result.add(element.trim());
            }
        }
        return result;
    }
}
