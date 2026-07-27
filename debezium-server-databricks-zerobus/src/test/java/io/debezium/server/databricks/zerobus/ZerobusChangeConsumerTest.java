/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.databricks.zerobus.ZerobusJsonStream;

import io.debezium.DebeziumException;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;

/**
 * Tests the gRPC route's batch handling by mocking the Databricks {@link ZerobusJsonStream} (the
 * approach for the gRPC route noted in issue #2279). We pre-seed the consumer's per-table stream map
 * so {@code handle()} never calls the real {@code ZerobusSdk} — verifying ingest, flush-per-batch,
 * tombstone skipping and fail-fast on ingest/flush errors, with no cloud and no live stream open.
 * <p>
 * Skipped when the SDK native library cannot be loaded (mocking {@code ZerobusJsonStream} touches
 * the SDK's native loader), mirroring how the SDK's own {@code StreamBuilderTest} guards itself.
 */
class ZerobusChangeConsumerTest {

    @BeforeAll
    static void requireNativeLibrary() {
        boolean loadable = true;
        try {
            Class.forName("com.databricks.zerobus.ZerobusSdk");
        }
        catch (Throwable t) {
            loadable = false;
        }
        assumeTrue(loadable, "Zerobus SDK / native library required to mock ZerobusJsonStream");
    }

    @Test
    void ingestsQualifiedRecordThenFlushes() throws Exception {
        ZerobusJsonStream stream = mock(ZerobusJsonStream.class);
        ZerobusChangeConsumer consumer = consumerWithStream("main.default.customers", stream);

        consumer.handle(events(event("{\"id\":1}", "main.default.customers")));

        verify(stream).ingestRecordOffset("{\"id\":1}");
        verify(stream).flush();
    }

    @Test
    void skipsTombstoneButStillFlushesTouchedStreams() throws Exception {
        ZerobusJsonStream stream = mock(ZerobusJsonStream.class);
        ZerobusChangeConsumer consumer = consumerWithStream("main.default.customers", stream);

        consumer.handle(events(event(null, "main.default.customers")));

        verify(stream, never()).ingestRecordOffset(org.mockito.ArgumentMatchers.anyString());
        // flush still runs over the (pre-seeded) stream map at end of batch
        verify(stream).flush();
    }

    @Test
    void wrapsIngestErrorAsDebeziumException() throws Exception {
        ZerobusJsonStream stream = mock(ZerobusJsonStream.class);
        doThrow(new RuntimeException("boom")).when(stream).ingestRecordOffset("{\"id\":1}");
        ZerobusChangeConsumer consumer = consumerWithStream("main.default.customers", stream);

        assertThatThrownBy(() -> consumer.handle(events(event("{\"id\":1}", "main.default.customers"))))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("main.default.customers");
    }

    // --- helpers -------------------------------------------------------------

    /** Builds a consumer with a configured default table and a pre-seeded stream map (no SDK call). */
    @SuppressWarnings("unchecked")
    private ZerobusChangeConsumer consumerWithStream(String table, ZerobusJsonStream stream) throws Exception {
        ZerobusChangeConsumer consumer = new ZerobusChangeConsumer();

        io.debezium.config.Configuration cfg = io.debezium.config.Configuration.create()
                .with("endpoint", "unused.example.com")
                .with("workspace.url", "https://dbc.example.com")
                .with("client.id", "c").with("client.secret", "s")
                .with("table", table)
                .build();
        set(consumer, "config", new ZerobusChangeConsumerConfig(cfg));

        Map<String, ZerobusJsonStream> streams = (Map<String, ZerobusJsonStream>) get(consumer, "streams");
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
