/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.storage.redis.RedisClient;
import io.debezium.util.IoUtil;
import io.smallrye.mutiny.tuples.Tuple2;

public class RedisMemoryThreshold {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisMemoryThreshold.class);

    private static final String INFO_MEMORY = "memory";
    private static final String INFO_MEMORY_SECTION_MAXMEMORY = "maxmemory";
    private static final String INFO_MEMORY_SECTION_USEDMEMORY = "used_memory";
    private static long accumulatedMemory = 0L;
    private static long previouslyUsedMemory = 0L;
    private static long totalProcessed = 0;

    private RedisClient client;
    private long memoryLimit = 0;
    private long maximumMemory = 0;

    public RedisMemoryThreshold(RedisClient client, RedisStreamChangeConsumerConfig config) {
        this.client = client;
        this.memoryLimit = 1024L * 1024 * config.getMemoryLimitMb();
    }

    /**
     * @param client
     */
    public void setRedisClient(RedisClient client) {
        this.client = client;
    }

    /**
     * Redis Enterprise samples the Redis database memory usage in an interval. Since the throughput of Redis is very big,
     * it is impossible to rely on delayed memory usage reading to prevent OOM.
     * In order to protect the Redis database and throttle down the sink we have created this mechanism:...
     * @param extraMemory   - Estimated size of a single record.
     * @param bufferSize    - Number of records in a batch.
     * @param bufferFillRate - Rate in which memory can be filled.
     * @return
     */
    public boolean checkMemory(long extraMemory, int bufferSize, int bufferFillRate) {
        Tuple2<Long, Long> memoryTuple = memoryTuple();

        if (totalProcessed + bufferSize >= Long.MAX_VALUE) {
            LOGGER.warn("Resetting the total processed records counter as it has reached its maximum value: {}", totalProcessed);
            totalProcessed = 0;
        }
        maximumMemory = memoryTuple.getItem2();

        if (maximumMemory == 0) {
            totalProcessed += bufferSize;
            LOGGER.debug("Total Processed Records: {}", totalProcessed);
            return true;
        }

        long extimatedBatchSize = extraMemory * bufferFillRate;
        long usedMemory = memoryTuple.getItem1();
        long prevAccumulatedMemory = accumulatedMemory;
        long prevPreviouslyUsedMemory = previouslyUsedMemory;
        long batchMemory = extraMemory * bufferSize;
        long diff = usedMemory - previouslyUsedMemory;
        if (diff == 0L) {
            // Redis reports 'used_memory' in coarse allocator steps, so an unchanged reading does not
            // prove nothing was written; the accumulator estimates what was sent but is not visible yet.
            accumulatedMemory += batchMemory;
        }
        else {
            // A changed reading reconciles the estimate: Redis has now accounted for what was written,
            // so only the batch about to be sent remains unaccounted for.
            previouslyUsedMemory = usedMemory;
            accumulatedMemory = batchMemory;
        }

        long estimatedUsedMemory = usedMemory + accumulatedMemory + extimatedBatchSize;

        // The accumulator only estimates writes Redis has not reported yet; those writes still landed in
        // Redis. So once the estimate alone would stop the sink, an unchanged 'used_memory' reading
        // disproves it - a Redis that really held this much would have reported a different figure.
        // Without this reconciliation the accumulator grows on every coarse-grained reading, is never
        // checked against reality, and eventually stops the sink for good while Redis is nearly empty.
        // Discarding the unproven part keeps back-pressure driven by what Redis actually reports.
        if (estimatedUsedMemory >= maximumMemory && usedMemory + extimatedBatchSize < maximumMemory) {
            LOGGER.debug(
                    "Discarding unconfirmed accumulated memory of {}: Redis still reports {} used of {}.",
                    getSizeInHumanReadableFormat(accumulatedMemory), getSizeInHumanReadableFormat(usedMemory),
                    getSizeInHumanReadableFormat(maximumMemory));
            previouslyUsedMemory = usedMemory;
            accumulatedMemory = batchMemory;
            estimatedUsedMemory = usedMemory + accumulatedMemory + extimatedBatchSize;
        }

        if (estimatedUsedMemory >= maximumMemory) {
            LOGGER.info(
                    "Sink memory threshold percentage was reached. Will retry; "
                            + "(estimated used memory size: {}, maxmemory: {}). Total Processed Records: {}",
                    getSizeInHumanReadableFormat(estimatedUsedMemory), getSizeInHumanReadableFormat(maximumMemory), totalProcessed);
            // The batch was not sent, so neither the accumulator nor the last observed reading may
            // advance. Rolling back only the accumulator would leave 'previouslyUsedMemory' updated,
            // making the next call see an unchanged reading and resume accumulating from there.
            accumulatedMemory = prevAccumulatedMemory;
            previouslyUsedMemory = prevPreviouslyUsedMemory;
            return false;
        }
        else {
            LOGGER.debug(
                    "Maximum reached: {}; Used Mem {}; Max Mem: {}; Accumulate Mem: {}; Estimated Used Mem: {}, Record Size: {}, NumRecInBuff: {}; Total Processed Records: {}",
                    (estimatedUsedMemory >= maximumMemory), getSizeInHumanReadableFormat(usedMemory), getSizeInHumanReadableFormat(maximumMemory),
                    getSizeInHumanReadableFormat(accumulatedMemory), getSizeInHumanReadableFormat(estimatedUsedMemory),
                    getSizeInHumanReadableFormat(extraMemory), bufferSize, totalProcessed + bufferSize);
            totalProcessed += bufferSize;
            return true;
        }
    }

    private Tuple2<Long, Long> memoryTuple() {
        String memory = client.info(INFO_MEMORY);
        Map<String, String> infoMemory = new HashMap<>();
        try {
            IoUtil.readLines(new ByteArrayInputStream(memory.getBytes(StandardCharsets.UTF_8)), line -> {
                String[] pair = line.split(":");
                if (pair.length == 2) {
                    infoMemory.put(pair[0], pair[1]);
                }
            });
        }
        catch (IOException e) {
            LOGGER.error("Cannot parse Redis 'info memory' result '{}'.", memory, e);
            return null;
        }

        Long usedMemory = parseLong(INFO_MEMORY_SECTION_USEDMEMORY, infoMemory.get(INFO_MEMORY_SECTION_USEDMEMORY));
        if (usedMemory == null) {
            usedMemory = 0L;
        }
        Long configuredMemory = parseLong(INFO_MEMORY_SECTION_MAXMEMORY, infoMemory.get(INFO_MEMORY_SECTION_MAXMEMORY));
        if (configuredMemory == null || (memoryLimit > 0 && configuredMemory > memoryLimit)) {
            configuredMemory = memoryLimit;
            if (configuredMemory > 0) {
                LOGGER.debug("Setting maximum memory size {}", getSizeInHumanReadableFormat(configuredMemory));
            }
        }
        return Tuple2.of(usedMemory, configuredMemory);
    }

    private Long parseLong(String name, String value) {
        if (value == null) {
            return null;
        }

        try {
            return Long.valueOf(value);
        }
        catch (NumberFormatException e) {
            LOGGER.debug("Cannot parse Redis 'info memory' field '{}' with value '{}'.", name, value);
            return null;
        }
    }

    /**
     * Formats a raw file size value into a human-readable string with appropriate units (B, KB, MB, GB).
     *
     * @param size The size value to be formatted.
     * @return A human-readable string representing the file size with units (B, KB, MB, GB).
     */
    public static String getSizeInHumanReadableFormat(Long size) {
        if (size == null) {
            return "Not configured";
        }

        final String[] units = { "B", "KB", "MB", "GB" };
        int unitIndex = 0;

        double sizeInUnit = size;

        if (size == 0) {
            return "0 B";
        }

        while (sizeInUnit >= 1024 && unitIndex < units.length - 1) {
            sizeInUnit /= 1024.0;
            unitIndex++;
        }

        return String.format("%.2f %s", sizeInUnit, units[unitIndex]);
    }
}
