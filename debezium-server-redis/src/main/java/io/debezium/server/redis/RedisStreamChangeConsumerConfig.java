/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.util.List;
import java.util.Set;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.config.Field.RangeValidator;
import io.debezium.storage.redis.RedisCommonConfig;
import io.debezium.util.Collect;

public class RedisStreamChangeConsumerConfig extends RedisCommonConfig {

    private static final String PROP_PREFIX = "debezium.sink.";

    private static final int DEFAULT_BATCH_SIZE = 500;
    private static final Field PROP_BATCH_SIZE = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "batch.size")
            .withDefault(DEFAULT_BATCH_SIZE);

    private static final String DEFAULT_NULL_KEY = "default";
    private static final Field PROP_NULL_KEY = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "null.key")
            .withDefault(DEFAULT_NULL_KEY);

    private static final String DEFAULT_NULL_VALUE = "default";
    private static final Field PROP_NULL_VALUE = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "null.value")
            .withDefault(DEFAULT_NULL_VALUE);

    static final String MESSAGE_FORMAT_COMPACT = "compact";
    static final String MESSAGE_FORMAT_EXTENDED = "extended";
    private static final Field PROP_MESSAGE_FORMAT = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "message.format")
            .withAllowedValues(Set.of(MESSAGE_FORMAT_COMPACT, MESSAGE_FORMAT_EXTENDED))
            .withDefault(MESSAGE_FORMAT_COMPACT);

    private static final int DEFAULT_MEMORY_LIMIT_MB = 0;
    private static final int DEFAULT_BUFFER_FILL_RATE = 30000;
    private static final Field PROP_MEMORY_LIMIT_MB = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "memory.limit.mb")
            .withDefault(DEFAULT_MEMORY_LIMIT_MB)
            .withValidation(RangeValidator.atLeast(0));
    private static final Field PROP_BUFFER_FILL_RATE = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "buffer.fill.rate")
            .withDefault(DEFAULT_BUFFER_FILL_RATE)
            .withValidation(RangeValidator.atLeast(0));

    private static final boolean DEFAULT_SKIP_HEARTBEAT_MESSAGES = true;
    private static final Field PROP_SKIP_HEARTBEAT_MESSAGES = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "skip.heartbeat.messages")
            .withDefault(DEFAULT_SKIP_HEARTBEAT_MESSAGES);

    /**
     * How many consecutive unchanged {@code used_memory} readings the memory threshold keeps
     * accumulating over before it stops adding to its estimate.
     * <p>
     * Redis reports {@code used_memory} in coarse allocator steps, so a short run of identical
     * readings is normal, and accumulating across them is what protects against OOM where the
     * figure is sampled on an interval. A reading that stops moving altogether is not normal,
     * and without a bound the estimate grows until it stops the sink while Redis is nearly empty.
     * <p>
     * Internal, so the bound can be raised for a workload that needs more tolerance without
     * becoming a documented knob.
     */
    private static final int DEFAULT_MAX_UNCHANGED_MEMORY_READINGS = 3;
    private static final Field PROP_MAX_UNCHANGED_MEMORY_READINGS = Field
            .createInternal("redis.max.unchanged.memory.readings")
            .withDefault(DEFAULT_MAX_UNCHANGED_MEMORY_READINGS)
            .withValidation(RangeValidator.atLeast(1));

    private int batchSize;
    private String nullKey;
    private String nullValue;
    private String messageFormat;
    private int memoryThreshold;
    private int memoryLimitMb;
    private int batchDelay;
    private int bufferFillRate;
    private boolean skipHeartbeatMessages;
    private int maxUnchangedMemoryReadings;

    public RedisStreamChangeConsumerConfig() {
        super();
    }

    public RedisStreamChangeConsumerConfig(Configuration config) {
        super(config, PROP_PREFIX);
    }

    @Override
    protected void init(Configuration config) {
        super.init(config);
        batchSize = config.getInteger(PROP_BATCH_SIZE);
        nullKey = config.getString(PROP_NULL_KEY);
        nullValue = config.getString(PROP_NULL_VALUE);
        messageFormat = config.getString(PROP_MESSAGE_FORMAT);
        memoryLimitMb = config.getInteger(PROP_MEMORY_LIMIT_MB);
        bufferFillRate = config.getInteger(PROP_BUFFER_FILL_RATE);
        skipHeartbeatMessages = config.getBoolean(PROP_SKIP_HEARTBEAT_MESSAGES);
        maxUnchangedMemoryReadings = config.getInteger(PROP_MAX_UNCHANGED_MEMORY_READINGS);
    }

    @Override
    protected List<Field> getAllConfigurationFields() {
        List<Field> fields = Collect.arrayListOf(PROP_BATCH_SIZE, PROP_NULL_KEY, PROP_NULL_VALUE, PROP_MESSAGE_FORMAT,
                PROP_SKIP_HEARTBEAT_MESSAGES, PROP_MAX_UNCHANGED_MEMORY_READINGS);
        fields.addAll(super.getAllConfigurationFields());
        return fields;
    }

    public int getBufferFillRate() {
        return bufferFillRate;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getBatchDelay() {
        return batchDelay;
    }

    public String getNullKey() {
        return nullKey;
    }

    public String getNullValue() {
        return nullValue;
    }

    public String getMessageFormat() {
        return messageFormat;
    }

    public int getMemoryThreshold() {
        return memoryThreshold;
    }

    public int getMemoryLimitMb() {
        return memoryLimitMb;
    }

    public boolean isSkipHeartbeatMessages() {
        return skipHeartbeatMessages;
    }

    public int getMaxUnchangedMemoryReadings() {
        return maxUnchangedMemoryReadings;
    }
}
