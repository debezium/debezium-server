/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.lang.reflect.Field;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.Header;
import io.debezium.storage.redis.RedisClient;
import io.debezium.util.Collect;

public class RedisMemoryThresholdTest {
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private static final String _5MB = String.valueOf(5 * 1024 * 1024);
    private static final String _10MB = String.valueOf(10 * 1024 * 1024);
    private static final String _20MB = String.valueOf(20 * 1024 * 1024);
    private static final long RECORD_SIZE = 2048L;
    private static final int BUFFER_SIZE = 500;
    private static final int RATE_PER_SECOND = 1000;

    private static final String HEARTBEAT_PREFIX = "__debezium-heartbeat";

    @Test
    public void testHeartbeatOnlyBatchCompletesWithoutInfiniteLoop() throws Exception {
        RedisStreamChangeConsumer consumer = new RedisStreamChangeConsumer();

        Configuration config = Configuration.from(Collect.hashMapOf(
                "debezium.sink.redis.address", "localhost:6379",
                "debezium.sink.redis.skip.heartbeat.messages", "true",
                "debezium.sink.redis.message.format", "compact"));
        RedisStreamChangeConsumerConfig consumerConfig = new RedisStreamChangeConsumerConfig(config);

        setField(consumer, RedisStreamChangeConsumer.class, "config", consumerConfig);
        setField(consumer, RedisStreamChangeConsumer.class, "heartbeatPrefix", HEARTBEAT_PREFIX);
        setField(consumer, RedisStreamChangeConsumer.class, "client", new RedisClientImpl(_10MB, _20MB));
        setField(consumer, RedisStreamChangeConsumer.class, "redisMemoryThreshold",
                new RedisMemoryThreshold(new RedisClientImpl(_10MB, _20MB), consumerConfig));

        CapturingEvents<BatchEvent> batch = new CapturingEvents<>() {
            @Override
            public List<BatchEvent> records() {
                return List.of(new HeartbeatBatchEvent());
            }

            @Override
            public String destination() {
                return HEARTBEAT_PREFIX + ".testc";
            }

            @Override
            public String source() {
                return "";
            }

            @Override
            public String engine() {
                return "";
            }
        };

        Awaitility.await()
                .atMost(3, TimeUnit.SECONDS)
                .until(consume(consumer, batch)::isDone);
    }

    private Future<?> consume(RedisStreamChangeConsumer consumer,
                              CapturingEvents<BatchEvent> changeEvents) {
        return executor.submit(() -> {
            try {
                consumer.handle(changeEvents);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void setField(Object target, Class<?> declaringClass, String fieldName, Object value)
            throws Exception {
        Field field = declaringClass.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    @Test
    public void testMemoryLimits() {
        Configuration config = Configuration.from(Collect.hashMapOf("debezium.sink.redis.address", "localhost",
                "debezium.sink.redis.rate.per.second", RATE_PER_SECOND));
        RedisMemoryThreshold redisMemoryThreshold = new RedisMemoryThreshold(new RedisClientImpl(_10MB, _20MB),
                new RedisStreamChangeConsumerConfig(config));
        for (int i = 0; i < 8; i++) {
            Assertions.assertTrue(redisMemoryThreshold.checkMemory(RECORD_SIZE, BUFFER_SIZE, RATE_PER_SECOND));
        }
        Assertions.assertFalse(redisMemoryThreshold.checkMemory(RECORD_SIZE, BUFFER_SIZE, RATE_PER_SECOND));
        redisMemoryThreshold.setRedisClient(new RedisClientImpl(_5MB, _20MB));
        Assertions.assertTrue(redisMemoryThreshold.checkMemory(RECORD_SIZE, BUFFER_SIZE, RATE_PER_SECOND));
    }

    private static class RedisClientImpl implements RedisClient {

        private String infoMemory;

        private RedisClientImpl(String usedMemoryBytes, String maxMemoryBytes) {
            this.infoMemory = (usedMemoryBytes == null ? "" : "used_memory:" + usedMemoryBytes + "\n")
                    + (maxMemoryBytes == null ? "" : "maxmemory:" + maxMemoryBytes);
        }

        @Override
        public String info(String section) {
            return infoMemory;
        }

        @Override
        public void disconnect() {
        }

        @Override
        public void close() {
        }

        @Override
        public String xadd(String key, Map<String, String> hash) {
            return null;
        }

        @Override
        public List<String> xadd(List<SimpleEntry<String, Map<String, String>>> hashes) {
            return null;
        }

        @Override
        public List<Map<String, String>> xrange(String key) {
            return null;
        }

        @Override
        public long xlen(String key) {
            return 0;
        }

        @Override
        public Map<String, String> hgetAll(String key) {
            return null;
        }

        @Override
        public long hset(byte[] key, byte[] field, byte[] value) {
            return 0;
        }

        @Override
        public long waitReplicas(int replicas, long timeout) {
            return 0;
        }

        @Override
        public String clientList() {
            return null;
        }
    }

    private static class HeartbeatBatchEvent implements BatchEvent {

        HeartbeatBatchEvent() {}

        @Override
        public Object key() {
            return null;
        }

        @Override
        public Object value() {
            return null;
        }

        @Override
        public Integer partition() {
            return null;
        }

        @Override
        public SourceRecord record() {
            return null;
        }

        @Override
        public void commit() {

        }

        @Override
        public List<Header<Object>> headers() {
            return Collections.emptyList();
        }
    }

    private static class NoOpRecordCommitter implements RecordCommitter<ChangeEvent<Object, Object>> {
        @Override
        public void markProcessed(ChangeEvent<Object, Object> record) throws InterruptedException {
        }

        @Override
        public void markProcessed(ChangeEvent<Object, Object> record, DebeziumEngine.Offsets offsets)
                throws InterruptedException {
        }

        @Override
        public void markBatchFinished() throws InterruptedException {
        }

        @Override
        public DebeziumEngine.Offsets buildOffsets() {
            return new DebeziumEngine.Offsets() {
                @Override
                public void set(String key, Object value) {
                }
            };
        }
    }
}
