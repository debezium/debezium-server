/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.Header;

class ZeroBusBatchWriterTest {

    @Test
    void successfulAckReturnsProcessedRecords() throws Exception {
        CapturingClient client = new CapturingClient();
        ZeroBusBatchWriter writer = writer(client, ZeroBusSinkConfigTest.baseConfig());
        List<ChangeEvent<Object, Object>> records = List.of(record("server.inventory.customers", "1", "{\"after\":{\"id\":1}}"));

        List<ChangeEvent<Object, Object>> processed = writer.write(records);

        assertThat(processed).containsExactlyElementsOf(records);
        assertThat(client.targets).containsExactly("main.bronze.customers");
        assertThat(client.records.get(0).operation()).isEqualTo(ZeroBusOperation.CHANGE);
        assertThat(client.records.get(0).idempotencyKey()).contains("server.inventory.customers|partition=0|key=1|headers=source.lsn=100");
    }

    @Test
    void sourceRecordOffsetDrivesIdempotencyKeyWhenAvailable() throws Exception {
        CapturingClient client = new CapturingClient();
        ZeroBusBatchWriter writer = writer(client, ZeroBusSinkConfigTest.baseConfig());

        writer.write(List.of(embeddedRecord(
                "server.inventory.customers",
                "1",
                "{\"after\":{\"id\":1}}",
                Map.of("server", "inventory"),
                Map.of("lsn", 123456789L, "txId", 42L, "event_serial_no", 3))));

        ZeroBusRecord written = client.records.get(0);
        assertThat(written.sourcePosition()).containsEntry("offset.lsn", "123456789");
        assertThat(written.sourcePosition()).containsEntry("offset.txId", "42");
        assertThat(written.sourcePosition()).containsEntry("offset.event_serial_no", "3");
        assertThat(written.idempotencyKey())
                .contains("source_position=")
                .contains("offset.lsn=123456789")
                .doesNotContain("headers=source.lsn=100");
    }

    @Test
    void tombstoneEventIsMappedWhenValueIsNull() throws Exception {
        CapturingClient client = new CapturingClient();
        ZeroBusBatchWriter writer = writer(client, ZeroBusSinkConfigTest.baseConfig());

        writer.write(List.of(record("server.inventory.customers", "1", null)));

        assertThat(client.records.get(0).operation()).isEqualTo(ZeroBusOperation.TOMBSTONE);
        assertThat(client.records.get(0).value()).isNull();
    }

    @Test
    void deleteEnvelopeOperationIsMappedFromJsonOpD() throws Exception {
        CapturingClient client = new CapturingClient();
        ZeroBusBatchWriter writer = writer(client, ZeroBusSinkConfigTest.baseConfig());

        writer.write(List.of(record("server.inventory.customers", "1", "{\"before\":{\"id\":1},\"after\":null,\"op\":\"d\"}")));

        assertThat(client.records.get(0).operation()).isEqualTo(ZeroBusOperation.DELETE);
        assertThat(client.records.get(0).value()).isEqualTo("{\"before\":{\"id\":1},\"after\":null,\"op\":\"d\"}");
    }

    @Test
    void droppedTombstoneIsMarkedProcessedWithoutWrite() throws Exception {
        ZeroBusSinkConfig config = new ZeroBusSinkConfig(ZeroBusSinkConfigTest.baseBuilder()
                .with("tombstone.handling.mode", "drop")
                .build());
        config.validate();
        CapturingClient client = new CapturingClient();

        List<ChangeEvent<Object, Object>> processed = writer(client, config)
                .write(List.of(record("server.inventory.customers", "1", null)));

        assertThat(processed).hasSize(1);
        assertThat(client.records).isEmpty();
    }

    @Test
    void partialAckFailsBatch() {
        ZeroBusClient client = (targetTable, records) -> ZeroBusWriteResult.partial(records.size(), 1);
        ZeroBusBatchWriter writer = writer(client, ZeroBusSinkConfigTest.baseConfig());

        assertThatThrownBy(() -> writer.write(List.of(
                record("server.inventory.customers", "1", "{\"after\":{\"id\":1}}"),
                record("server.inventory.customers", "2", "{\"after\":{\"id\":2}}"))))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("partially acknowledged 1 result(s) for 2 record(s)");
    }

    @Test
    void preservesSourceOrderAcrossInterleavedTargetTables() throws Exception {
        CapturingClient client = new CapturingClient();
        ZeroBusBatchWriter writer = writer(client, ZeroBusSinkConfigTest.baseConfig());

        writer.write(List.of(
                record("server.inventory.customers", "1", "{\"after\":{\"id\":1}}"),
                record("server.inventory.orders", "10", "{\"after\":{\"id\":10}}"),
                record("server.inventory.customers", "2", "{\"after\":{\"id\":2}}")));

        assertThat(client.targets).containsExactly(
                "main.bronze.customers",
                "main.bronze.orders",
                "main.bronze.customers");
        assertThat(client.records).extracting(ZeroBusRecord::destination).containsExactly(
                "server.inventory.customers",
                "server.inventory.orders",
                "server.inventory.customers");
    }

    @Test
    void batchesConsecutiveRecordsForSameTargetTable() throws Exception {
        CapturingClient client = new CapturingClient();
        ZeroBusSinkConfig config = new ZeroBusSinkConfig(ZeroBusSinkConfigTest.baseBuilder()
                .with("batch.size", "2")
                .build());
        config.validate();

        writer(client, config).write(List.of(
                record("server.inventory.customers", "1", "{\"after\":{\"id\":1}}"),
                record("server.inventory.customers", "2", "{\"after\":{\"id\":2}}"),
                record("server.inventory.customers", "3", "{\"after\":{\"id\":3}}")));

        assertThat(client.targets).containsExactly("main.bronze.customers", "main.bronze.customers");
        assertThat(client.writeSizes).containsExactly(2, 1);
    }

    @Test
    void retryableFailureRetriesWithSameIdempotencyKey() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        List<String> idempotencyKeys = new ArrayList<>();
        ZeroBusClient client = (targetTable, records) -> {
            attempts.incrementAndGet();
            idempotencyKeys.add(records.get(0).idempotencyKey());
            if (attempts.get() == 1) {
                throw new ZeroBusRetriableException("retry");
            }
            return ZeroBusWriteResult.acknowledged(records.size());
        };
        ZeroBusSinkConfig config = new ZeroBusSinkConfig(ZeroBusSinkConfigTest.baseBuilder()
                .with("retry.interval.ms", "0")
                .build());
        config.validate();

        writer(client, config).write(List.of(record("server.inventory.customers", "1", "{\"after\":{\"id\":1}}")));

        assertThat(attempts).hasValue(2);
        assertThat(idempotencyKeys).hasSize(2).containsOnly(idempotencyKeys.get(0));
    }

    @Test
    void retriesConfigCountsRetriesAfterInitialAttempt() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        ZeroBusClient client = (targetTable, records) -> {
            if (attempts.incrementAndGet() == 1) {
                throw new ZeroBusRetriableException("retry once");
            }
            return ZeroBusWriteResult.acknowledged(records.size());
        };
        ZeroBusSinkConfig config = new ZeroBusSinkConfig(ZeroBusSinkConfigTest.baseBuilder()
                .with("retries", "1")
                .with("retry.interval.ms", "0")
                .build());
        config.validate();

        writer(client, config).write(List.of(record("server.inventory.customers", "1", "{\"after\":{\"id\":1}}")));

        assertThat(attempts).hasValue(2);
    }

    @Test
    void idempotencyKeyEscapesDelimiterBearingValues() throws Exception {
        CapturingClient client = new CapturingClient();
        ZeroBusBatchWriter writer = writer(client, ZeroBusSinkConfigTest.baseConfig());

        writer.write(List.of(
                record("srv", "|partition=0|key=r2", "{\"after\":{\"id\":1}}", List.of()),
                record("srv|partition=0|key=", "r2", "{\"after\":{\"id\":2}}", List.of())));

        assertThat(client.records)
                .extracting(ZeroBusRecord::idempotencyKey)
                .doesNotHaveDuplicates();
    }

    @Test
    void unknownEnvelopeOperationMapsToChange() throws Exception {
        CapturingClient client = new CapturingClient();
        ZeroBusBatchWriter writer = writer(client, ZeroBusSinkConfigTest.baseConfig());

        writer.write(List.of(record("server.inventory.customers", "1", "{\"op\":\"t\"}")));

        assertThat(client.records.get(0).operation()).isEqualTo(ZeroBusOperation.CHANGE);
    }

    @Test
    void idempotencyCanBeDisabled() throws Exception {
        ZeroBusSinkConfig config = new ZeroBusSinkConfig(ZeroBusSinkConfigTest.baseBuilder()
                .with("idempotency.mode", "none")
                .build());
        config.validate();
        CapturingClient client = new CapturingClient();

        writer(client, config).write(List.of(record("server.inventory.customers", "1", "{\"after\":{\"id\":1}}")));

        assertThat(client.records.get(0).idempotencyKey()).isNull();
    }

    private static ZeroBusBatchWriter writer(ZeroBusClient client, ZeroBusSinkConfig config) {
        ZeroBusTableRouter router = new ZeroBusTableRouter(config);
        return new ZeroBusBatchWriter(config, client, new ZeroBusRecordMapper(router, config));
    }

    @SuppressWarnings("unchecked")
    private static ChangeEvent<Object, Object> record(String destination, String key, String value) {
        Header<Object> header = mock(Header.class);
        when(header.getKey()).thenReturn("source.lsn");
        when(header.getValue()).thenReturn("100");
        return record(destination, key, value, List.of(header));
    }

    @SuppressWarnings("unchecked")
    private static ChangeEvent<Object, Object> record(String destination, String key, String value, List<Header<Object>> headers) {
        ChangeEvent<Object, Object> record = mock(ChangeEvent.class);
        when(record.destination()).thenReturn(destination);
        when(record.partition()).thenReturn(0);
        when(record.key()).thenReturn(key);
        when(record.value()).thenReturn(value);
        when(record.headers()).thenReturn(headers);
        return record;
    }

    @SuppressWarnings("unchecked")
    private static ChangeEvent<Object, Object> embeddedRecord(String destination, String key, String value,
                                                              Map<String, ?> sourcePartition, Map<String, ?> sourceOffset) {
        EmbeddedEngineChangeEvent<Object, Object, Object> record = mock(EmbeddedEngineChangeEvent.class);
        Header<Object> header = mock(Header.class);
        SourceRecord sourceRecord = new SourceRecord(
                sourcePartition,
                sourceOffset,
                destination,
                0,
                null,
                key,
                null,
                value);
        when(header.getKey()).thenReturn("source.lsn");
        when(header.getValue()).thenReturn("100");
        when(record.sourceRecord()).thenReturn(sourceRecord);
        when(record.destination()).thenReturn(destination);
        when(record.partition()).thenReturn(0);
        when(record.key()).thenReturn(key);
        when(record.value()).thenReturn(value);
        when(record.headers()).thenReturn(List.of(header));
        return record;
    }

    private static class CapturingClient implements ZeroBusClient {
        private final List<String> targets = new ArrayList<>();
        private final List<ZeroBusRecord> records = new ArrayList<>();
        private final List<Integer> writeSizes = new ArrayList<>();

        @Override
        public ZeroBusWriteResult write(String targetTable, List<ZeroBusRecord> records) {
            targets.add(targetTable);
            writeSizes.add(records.size());
            this.records.addAll(records);
            return ZeroBusWriteResult.acknowledged(records.size());
        }
    }
}
