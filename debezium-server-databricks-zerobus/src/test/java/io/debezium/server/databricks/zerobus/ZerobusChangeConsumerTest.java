/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.databricks.zerobus.metrics.ZerobusSinkMetrics;

/**
 * Tests the gRPC route's batch handling by mocking {@link ZerobusStreamHandle} (the approach for the
 * gRPC route noted in issue #2279). We pre-seed the consumer's per-table stream map so
 * {@code handle()} never calls the real {@code ZerobusSdk} — verifying ingest, flush-per-batch,
 * tombstone skipping and fail-fast on ingest/flush errors, with no cloud and no live stream open.
 * <p>
 * Mocking the sink's own interface rather than the SDK's {@code ZerobusJsonStream} keeps these tests
 * off the SDK's native loader, so they run on a platform with no native library available.
 */
class ZerobusChangeConsumerTest {

    @SuppressWarnings("unchecked")
    private static ZerobusStreamHandle<String> mockStream() {
        return mock(ZerobusStreamHandle.class);
    }

    @Test
    void ingestsQualifiedRecordThenFlushes() throws Exception {
        ZerobusStreamHandle<String> stream = mockStream();
        ZerobusChangeConsumer consumer = consumerWithStream("main.default.customers", stream);

        consumer.handle(events(event("{\"id\":1}", "main.default.customers")));

        verify(stream).ingest("{\"id\":1}");
        verify(stream).flush();
    }

    @Test
    void skipsTombstoneAndFlushesNothingWhenNoStreamTouched() throws Exception {
        ZerobusStreamHandle<String> stream = mockStream();
        ZerobusChangeConsumer consumer = consumerWithStream("main.default.customers", stream);

        consumer.handle(events(event(null, "main.default.customers")));

        verify(stream, never()).ingest(org.mockito.ArgumentMatchers.anyString());
        // a tombstone-only batch touches no stream, so nothing is flushed (only touched streams flush)
        verify(stream, never()).flush();
    }

    @Test
    void flushesOnlyTheStreamsTouchedInTheBatch() throws Exception {
        ZerobusStreamHandle<String> customers = mockStream();
        ZerobusStreamHandle<String> orders = mockStream();
        ZerobusChangeConsumer consumer = consumerWithStream("main.default.customers", customers);
        // pre-seed a second, untouched stream
        @SuppressWarnings("unchecked")
        Map<String, ZerobusStreamHandle<String>> streams = (Map<String, ZerobusStreamHandle<String>>) get(consumer, "streams");
        streams.put("main.default.orders", orders);

        // only customers receives a record this batch
        consumer.handle(events(event("{\"id\":1}", "main.default.customers")));

        verify(customers).flush();
        verify(orders, never()).flush(); // untouched stream must not be flushed
    }

    @Test
    void commitsOffsetsOnlyAfterTheStreamIsFlushed() throws Exception {
        ZerobusStreamHandle<String> stream = mockStream();
        ZerobusChangeConsumer consumer = consumerWithStream("main.default.customers", stream);

        BatchEvent record = mock(BatchEvent.class);
        org.mockito.Mockito.when(record.value()).thenReturn("{\"id\":1}");
        org.mockito.Mockito.when(record.destination()).thenReturn("main.default.customers");

        consumer.handle(events(record));

        // The offset must be acknowledged only after the batch is durable: flush first, commit second.
        // Committing before the flush would risk dropping buffered records on a crash (at-least-once).
        org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(stream, record);
        inOrder.verify(stream).ingest("{\"id\":1}");
        inOrder.verify(stream).flush();
        inOrder.verify(record).commit();
    }

    @Test
    void wrapsIngestErrorAsDebeziumException() throws Exception {
        ZerobusStreamHandle<String> stream = mockStream();
        doThrow(new RuntimeException("boom")).when(stream).ingest("{\"id\":1}");
        ZerobusChangeConsumer consumer = consumerWithStream("main.default.customers", stream);

        assertThatThrownBy(() -> consumer.handle(events(event("{\"id\":1}", "main.default.customers"))))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("main.default.customers");
    }

    @Test
    void recordsIngestAndFlushMetricsOnSuccess() throws Exception {
        ZerobusStreamHandle<String> stream = mockStream();
        ZerobusChangeConsumer consumer = consumerWithStream("main.default.customers", stream);
        ZerobusSinkMetrics metrics = mock(ZerobusSinkMetrics.class);
        set(consumer, "metrics", metrics);

        consumer.handle(events(event("{\"id\":1}", "main.default.customers")));

        verify(metrics).recordIngested(null, -1L); // no typed envelope in this event → null op / -1 ts
        verify(metrics).flushed(anyLong());
    }

    @Test
    void recordsSkippedMetricForTombstone() throws Exception {
        ZerobusStreamHandle<String> stream = mockStream();
        ZerobusChangeConsumer consumer = consumerWithStream("main.default.customers", stream);
        ZerobusSinkMetrics metrics = mock(ZerobusSinkMetrics.class);
        set(consumer, "metrics", metrics);

        consumer.handle(events(event(null, "main.default.customers")));

        verify(metrics).recordSkipped();
        verify(metrics, never()).recordIngested(org.mockito.ArgumentMatchers.any(), anyLong());
    }

    @Test
    void recordsErrorMetricWhenIngestFails() throws Exception {
        ZerobusStreamHandle<String> stream = mockStream();
        doThrow(new RuntimeException("boom")).when(stream).ingest(eq("{\"id\":1}"));
        ZerobusChangeConsumer consumer = consumerWithStream("main.default.customers", stream);
        ZerobusSinkMetrics metrics = mock(ZerobusSinkMetrics.class);
        set(consumer, "metrics", metrics);

        assertThatThrownBy(() -> consumer.handle(events(event("{\"id\":1}", "main.default.customers"))))
                .isInstanceOf(DebeziumException.class);
        verify(metrics).recordError();
    }

    // --- helpers -------------------------------------------------------------

    /** Builds a consumer with a configured default table and a pre-seeded stream map (no SDK call). */
    @SuppressWarnings("unchecked")
    private ZerobusChangeConsumer consumerWithStream(String table, ZerobusStreamHandle<String> stream) throws Exception {
        ZerobusChangeConsumer consumer = new ZerobusChangeConsumer();

        io.debezium.config.Configuration cfg = io.debezium.config.Configuration.create()
                .with("endpoint", "unused.example.com")
                .with("workspace.url", "https://dbc.example.com")
                .with("client.id", "c").with("client.secret", "s")
                .with("table", table)
                .build();
        set(consumer, "config", new ZerobusChangeConsumerConfig(cfg));

        Map<String, ZerobusStreamHandle<String>> streams = (Map<String, ZerobusStreamHandle<String>>) get(consumer, "streams");
        streams.put(table, stream);
        return consumer;
    }

    private static void set(Object target, String field, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(field);
        f.setAccessible(true);
        f.set(target, value);
    }

    private static Object get(Object target, String field) throws Exception {
        Field f = target.getClass().getDeclaredField(field);
        f.setAccessible(true);
        return f.get(target);
    }

    private static BatchEvent event(String value, String destination) {
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
                return destination;
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
