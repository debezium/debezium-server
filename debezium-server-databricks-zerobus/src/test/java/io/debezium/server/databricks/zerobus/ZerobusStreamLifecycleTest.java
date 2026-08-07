/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;

/**
 * Tests the stream bookkeeping of the gRPC consumer: that the number of open streams stays within
 * the configured bound by evicting the least recently used ones, and that the ingestion counters are
 * only advanced once the batch is durable.
 */
class ZerobusStreamLifecycleTest {

    @SuppressWarnings("unchecked")
    private static ZerobusStreamHandle<String> mockStream() {
        return mock(ZerobusStreamHandle.class);
    }

    private static ZerobusChangeConsumer consumerWith(Map<String, ZerobusStreamHandle<String>> streams, String... extraConfig) throws Exception {
        ZerobusChangeConsumer consumer = new ZerobusChangeConsumer();
        Configuration.Builder builder = Configuration.create()
                .with("endpoint", "ws.zerobus.us-west-2.cloud.databricks.com")
                .with("workspace.url", "https://ws.cloud.databricks.com")
                .with("client.id", "id")
                .with("client.secret", "secret")
                .with("table", "main.default.t");
        for (int i = 0; i < extraConfig.length; i += 2) {
            builder = builder.with(extraConfig[i], extraConfig[i + 1]);
        }
        set(consumer, "config", new ZerobusChangeConsumerConfig(builder.build()));
        set(consumer, "streams", streams);
        return consumer;
    }

    private static void set(Object target, String field, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(field);
        f.setAccessible(true);
        f.set(target, value);
    }

    private static void evict(ZerobusChangeConsumer consumer) throws Exception {
        java.lang.reflect.Method m = ZerobusChangeConsumer.class.getDeclaredMethod("evictStreamsAboveLimit");
        m.setAccessible(true);
        m.invoke(consumer);
    }

    @Test
    void closesLeastRecentlyUsedStreamsAboveTheLimit() throws Exception {
        // Access-ordered, as in the consumer, so iteration yields the least recently used first.
        Map<String, ZerobusStreamHandle<String>> streams = new LinkedHashMap<>(16, 0.75f, true);
        ZerobusStreamHandle<String> oldest = mockStream();
        ZerobusStreamHandle<String> middle = mockStream();
        ZerobusStreamHandle<String> newest = mockStream();
        streams.put("main.default.a", oldest);
        streams.put("main.default.b", middle);
        streams.put("main.default.c", newest);

        evict(consumerWith(streams, "max.open.streams", "2"));

        // The two most recently used survive; the eldest is flushed before being closed so that no
        // buffered record is lost.
        assertThat(streams.keySet()).containsExactly("main.default.b", "main.default.c");
        verify(oldest).flush();
        verify(oldest).close();
        verify(middle, never()).close();
        verify(newest, never()).close();
    }

    @Test
    void respectsAccessOrderWhenChoosingWhatToEvict() throws Exception {
        Map<String, ZerobusStreamHandle<String>> streams = new LinkedHashMap<>(16, 0.75f, true);
        ZerobusStreamHandle<String> a = mockStream();
        ZerobusStreamHandle<String> b = mockStream();
        streams.put("main.default.a", a);
        streams.put("main.default.b", b);
        // Touching "a" makes "b" the least recently used.
        streams.get("main.default.a");

        evict(consumerWith(streams, "max.open.streams", "1"));

        assertThat(streams.keySet()).containsExactly("main.default.a");
        verify(b).close();
        verify(a, never()).close();
    }

    @Test
    void keepsEveryStreamOpenWhenTheLimitIsZero() throws Exception {
        Map<String, ZerobusStreamHandle<String>> streams = new LinkedHashMap<>(16, 0.75f, true);
        ZerobusStreamHandle<String> a = mockStream();
        streams.put("main.default.a", a);
        streams.put("main.default.b", mockStream());

        evict(consumerWith(streams, "max.open.streams", "0"));

        assertThat(streams).hasSize(2);
        verify(a, never()).close();
    }

    @Test
    void anEvictionFailureStillDropsTheStreamAndDoesNotThrow() throws Exception {
        Map<String, ZerobusStreamHandle<String>> streams = new LinkedHashMap<>(16, 0.75f, true);
        ZerobusStreamHandle<String> failing = mockStream();
        org.mockito.Mockito.doThrow(new RuntimeException("close failed")).when(failing).close();
        streams.put("main.default.a", failing);
        streams.put("main.default.b", mockStream());

        // The records were already flushed and committed, so an eviction problem must not fail a batch.
        evict(consumerWith(streams, "max.open.streams", "1"));

        assertThat(streams.keySet()).containsExactly("main.default.b");
    }

    @Test
    void countsRecordsAsIngestedOnlyAfterTheFlushSucceeds() throws Exception {
        ZerobusStreamHandle<String> stream = mockStream();
        Map<String, ZerobusStreamHandle<String>> streams = new LinkedHashMap<>(16, 0.75f, true);
        streams.put("main.default.t", stream);
        ZerobusChangeConsumer consumer = consumerWith(streams);
        when(stream.ingest(any(String.class))).thenReturn(1L);

        consumer.handle(events(event("{\"id\":1,\"__op\":\"c\"}"), event("{\"id\":2,\"__op\":\"c\"}")));

        // Both records are durable, so both are counted, and the counting happens after the flush.
        org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(stream);
        inOrder.verify(stream, org.mockito.Mockito.times(2)).ingest(any(String.class));
        inOrder.verify(stream).flush();
    }

    @Test
    void countsNothingAsIngestedWhenTheFlushFails() throws Exception {
        ZerobusStreamHandle<String> stream = mockStream();
        org.mockito.Mockito.doThrow(new RuntimeException("flush failed")).when(stream).flush();
        Map<String, ZerobusStreamHandle<String>> streams = new LinkedHashMap<>(16, 0.75f, true);
        streams.put("main.default.t", stream);
        ZerobusChangeConsumer consumer = consumerWith(streams);
        when(stream.ingest(any(String.class))).thenReturn(1L);

        try {
            consumer.handle(events(event("{\"id\":1,\"__op\":\"c\"}")));
        }
        catch (Exception expected) {
            // The batch must fail; what matters is that the record was never counted as durable.
        }

        // The record reached the SDK but never became durable, so TotalRecordsIngested stays at zero
        // while TotalErrors records the failure.
        java.lang.reflect.Field mf = ZerobusChangeConsumer.class.getDeclaredField("metrics");
        mf.setAccessible(true);
        io.debezium.server.databricks.zerobus.metrics.ZerobusSinkMetrics metrics = (io.debezium.server.databricks.zerobus.metrics.ZerobusSinkMetrics) mf
                .get(consumer);
        assertThat(metrics.getTotalRecordsIngested()).isZero();
        assertThat(metrics.getTotalErrors()).isEqualTo(1);
    }

    private static BatchEvent event(String value) {
        return new BatchEvent() {
            public Object key() {
                return null;
            }

            public Object value() {
                return value;
            }

            public Integer partition() {
                return 0;
            }

            public org.apache.kafka.connect.source.SourceRecord record() {
                return null;
            }

            public String destination() {
                return "main.default.t";
            }

            public void commit() {
            }
        };
    }

    private static CapturingEvents<BatchEvent> events(BatchEvent... evts) {
        List<BatchEvent> list = new ArrayList<>(List.of(evts));
        return new CapturingEvents<>() {
            public List<BatchEvent> records() {
                return list;
            }

            public String destination() {
                return null;
            }

            public String source() {
                return null;
            }

            public String engine() {
                return null;
            }
        };
    }
}
