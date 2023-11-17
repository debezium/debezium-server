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
    private static long total_processed = 0;

    private RedisClient client;
    private long memoryLimit;

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
     * @param extraMemory   - Estimated size of a single record.
     * @param bufferSize    - Number of records in a batch.
     * @param ratePerSecond - Rate in which memory can be filled.
     * @return
     */
    public boolean checkMemory(long extraMemory, int bufferSize, int ratePerSecond) {
        Tuple2<Long, Long> memoryTuple = memoryTuple(memoryLimit);
        long maxMemory = memoryTuple.getItem2();

        long extimatedBatchSize = extraMemory * ratePerSecond;
        long usedMemory = memoryTuple.getItem1();
        long prevAccumulatedMemory = accumulatedMemory;
        long diff = usedMemory - previouslyUsedMemory;
        if (diff == 0L) {
            accumulatedMemory += extraMemory * bufferSize;
        }
        else {
            previouslyUsedMemory = usedMemory;
            accumulatedMemory = extraMemory * bufferSize;
        }
        long estimatedUsedMemory = usedMemory + accumulatedMemory + extimatedBatchSize;

        if (estimatedUsedMemory >= maxMemory) {
            LOGGER.info(
                    "Sink memory threshold percentage was reached. Will retry; "
                            + "(estimated used memory size: {}, maxmemory: {}). Total Processed Records: {}",
                    humanReadableSize(estimatedUsedMemory), humanReadableSize(maxMemory), total_processed);
            accumulatedMemory = prevAccumulatedMemory;
            return false;
        }
        else {
            LOGGER.debug(
                    "Maximum reached: {}; Used Mem {}; Max Mem: {}; Accumulate Mem: {}; Estimated Used Mem: {}, Record Size: {}, NumRecInBuff: {}; Total Processed Records: {}",
                    (estimatedUsedMemory >= maxMemory), humanReadableSize(usedMemory), humanReadableSize(maxMemory),
                    humanReadableSize(accumulatedMemory), humanReadableSize(estimatedUsedMemory),
                    humanReadableSize(extraMemory), bufferSize, total_processed + bufferSize);
            total_processed += bufferSize;
            return true;
        }
    }

    private Tuple2<Long, Long> memoryTuple(long defaultMaxMemory) {
        String memory = client.info(INFO_MEMORY);
        LOGGER.trace(memory);
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
        Long maxMemory = parseLong(INFO_MEMORY_SECTION_MAXMEMORY, infoMemory.get(INFO_MEMORY_SECTION_MAXMEMORY));

        if (maxMemory == null || (defaultMaxMemory > 0 && maxMemory > defaultMaxMemory)) {
            maxMemory = defaultMaxMemory;
            LOGGER.debug("Setting maximum memory size {}", maxMemory);
        }
        return Tuple2.of(usedMemory, maxMemory);
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

    private String humanReadableSize(long size) {
        if (size < 1024) {
            return size + " B";
        }
        else if (size < 1024 * 1024) {
            return String.format("%.2f KB", size / 1024.0);
        }
        else if (size < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", size / (1024.0 * 1024.0));
        }
        else {
            return String.format("%.2f GB", size / (1024.0 * 1024.0 * 1024.0));
        }
    }
}
