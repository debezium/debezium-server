/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DynamicMessage;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.engine.Header;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;

class ZerobusEnvelopeTest {

    private final ObjectMapper jsonMapper = new ObjectMapper();

    @Test
    void mapsSourcePositionAndDeterministicIdentity() {
        ZerobusEnvelopeMapper mapper = new ZerobusEnvelopeMapper(configWith());
        BatchEvent event = event("main.bronze.customers", "{\"id\":1}", "{\"op\":\"c\",\"after\":{\"id\":1}}", 0);
        SourceRecord sourceRecord = new SourceRecord(
                Map.of("server", "inventory"),
                Map.of("lsn", 123456789L, "event_serial_no", 3),
                event.destination(),
                event.partition(),
                null,
                event.key(),
                null,
                event.value());
        when(event.record()).thenReturn(sourceRecord);

        ZerobusEnvelope envelope = mapper.map(event, "main.bronze.customers");

        assertThat(envelope.operation()).isEqualTo(ZerobusOperation.CREATE);
        assertThat(envelope.sourcePosition())
                .containsEntry("partition.server", "inventory")
                .containsEntry("offset.lsn", "123456789")
                .containsEntry("offset.event_serial_no", "3");
        assertThat(envelope.idempotencyKey()).contains("source_position=").doesNotContain("headers=");
        assertThat(mapper.map(event, "main.bronze.customers").idempotencyKey()).isEqualTo(envelope.idempotencyKey());
    }

    @Test
    void mapsTombstoneAsDistinctOperation() {
        ZerobusEnvelope envelope = new ZerobusEnvelopeMapper(configWith())
                .map(event("main.bronze.customers", "{\"id\":1}", null, 0), "main.bronze.customers");

        assertThat(envelope.operation()).isEqualTo(ZerobusOperation.TOMBSTONE);
        assertThat(envelope.value()).isNull();
        assertThat(envelope.idempotencyKey()).isNotBlank();
    }

    @Test
    void serializesJsonEnvelopeWithCanonicalFlexibleFields() throws Exception {
        Map<String, String> sourcePosition = new LinkedHashMap<>();
        sourcePosition.put("offset.lsn", "42");
        ZerobusEnvelope envelope = envelope(sourcePosition);

        JsonNode json = jsonMapper.readTree(new ZerobusJsonEnvelopeSerializer(configWith()).serialize(envelope));

        assertThat(json.path("operation").asText()).isEqualTo("update");
        assertThat(json.path("key").asText()).isEqualTo("{\"id\":1}");
        assertThat(json.path("value").asText()).isEqualTo("{\"after\":{\"id\":1}}");
        assertThat(json.path("source_position").asText()).isEqualTo("{\"offset.lsn\":\"42\"}");
    }

    @Test
    void serializesProtobufEnvelopeAgainstPublishedDescriptor() throws Exception {
        ZerobusProtobufEnvelopeSerializer serializer = new ZerobusProtobufEnvelopeSerializer();
        DynamicMessage message = DynamicMessage.parseFrom(serializer.descriptor(), serializer.serialize(envelope(Map.of())));

        assertThat(message.getField(serializer.descriptor().findFieldByName("operation"))).isEqualTo("update");
        assertThat(message.getField(serializer.descriptor().findFieldByName("key"))).isEqualTo("{\"id\":1}");
        assertThat(serializer.descriptorProto().getFieldList())
                .extracting(com.google.protobuf.DescriptorProtos.FieldDescriptorProto::getName)
                .containsExactly("target_table", "destination", "partition", "operation", "idempotency_key",
                        "key", "value", "source_position", "headers");
    }

    @Test
    void preservesSourceOrderAcrossInterleavedTablesAndCommitsAfterAcknowledgement() throws Exception {
        ZerobusStreamHandle<String> customers = mockStream();
        ZerobusStreamHandle<String> orders = mockStream();
        when(customers.ingest(anyString())).thenReturn(10L, 30L);
        when(orders.ingest(anyString())).thenReturn(20L);
        ZerobusChangeConsumer consumer = consumer(configWith(), Map.of(
                "main.bronze.customers", customers,
                "main.bronze.orders", orders),
                Map.of());
        BatchEvent first = event("main.bronze.customers", "1", "{\"op\":\"c\"}", 0);
        BatchEvent second = event("main.bronze.orders", "10", "{\"op\":\"u\"}", 0);
        BatchEvent third = event("main.bronze.customers", "2", "{\"op\":\"d\"}", 0);

        consumer.handle(events(first, second, third));

        InOrder order = inOrder(customers, orders, first, second, third);
        order.verify(customers).ingest(anyString());
        order.verify(customers).waitForOffset(10L);
        order.verify(orders).ingest(anyString());
        order.verify(orders).waitForOffset(20L);
        order.verify(customers).ingest(anyString());
        order.verify(customers).waitForOffset(30L);
        order.verify(first).commit();
        order.verify(second).commit();
        order.verify(third).commit();
    }

    @Test
    void rejectsOversizedRecordBeforeIngestionOrOffsetCommit() throws Exception {
        ZerobusStreamHandle<String> stream = mockStream();
        ZerobusChangeConsumer consumer = consumer(configWith("max.record.bytes", "80"),
                Map.of("main.bronze.customers", stream), Map.of());
        BatchEvent event = event("main.bronze.customers", "1", "{\"op\":\"c\",\"after\":{\"payload\":\""
                + "x".repeat(200) + "\"}}", 0);

        assertThatThrownBy(() -> consumer.handle(events(event)))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("max.record.bytes=80")
                .hasMessageContaining("main.bronze.customers");
        verify(stream, never()).ingest(anyString());
        verify(event, never()).commit();
    }

    @Test
    void writesTombstoneEnvelopeByDefaultAndCanDropItExplicitly() throws Exception {
        ZerobusStreamHandle<String> stream = mockStream();
        when(stream.ingest(anyString())).thenReturn(7L);
        ZerobusChangeConsumer consumer = consumer(configWith(), Map.of("main.bronze.customers", stream), Map.of());
        BatchEvent tombstone = event("main.bronze.customers", "1", null, 0);

        consumer.handle(events(tombstone));

        ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);
        verify(stream).ingest(payload.capture());
        assertThat(jsonMapper.readTree(payload.getValue()).path("operation").asText()).isEqualTo("tombstone");
        verify(stream).waitForOffset(7L);
        verify(tombstone).commit();

        ZerobusStreamHandle<String> droppedStream = mockStream();
        ZerobusChangeConsumer droppingConsumer = consumer(configWith("tombstone.handling.mode", "drop"),
                Map.of("main.bronze.customers", droppedStream), Map.of());
        BatchEvent dropped = event("main.bronze.customers", "1", null, 0);
        droppingConsumer.handle(events(dropped));
        verify(droppedStream, never()).ingest(anyString());
        verify(dropped).commit();
    }

    @Test
    void writesProtobufAndWaitsForItsAssignedOffset() throws Exception {
        ZerobusStreamHandle<byte[]> stream = mockStream();
        when(stream.ingest(any(byte[].class))).thenReturn(99L);
        ZerobusChangeConsumer consumer = consumer(configWith("record.format", "protobuf"), Map.of(),
                Map.of("main.bronze.customers", stream));
        BatchEvent event = event("main.bronze.customers", "1", "{\"op\":\"u\"}", 0);

        consumer.handle(events(event));

        ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
        verify(stream).ingest(payload.capture());
        DynamicMessage message = DynamicMessage.parseFrom(new ZerobusProtobufEnvelopeSerializer().descriptor(), payload.getValue());
        assertThat(message.getField(message.getDescriptorForType().findFieldByName("operation"))).isEqualTo("update");
        verify(stream).waitForOffset(99L);
        verify(event).commit();
    }

    @Test
    void appliesSinkNativeDestinationFilterBeforeIngestion() throws Exception {
        ZerobusStreamHandle<String> stream = mockStream();
        ZerobusChangeConsumer consumer = consumer(configWith("filter.destination.regex", "main\\.bronze\\.customers"),
                Map.of("main.bronze.orders", stream), Map.of());
        BatchEvent event = event("main.bronze.orders", "1", "{\"op\":\"c\"}", 0);

        consumer.handle(events(event));

        verify(stream, never()).ingest(anyString());
        verify(event).commit();
    }

    private static ZerobusEnvelope envelope(Map<String, String> sourcePosition) {
        return new ZerobusEnvelope(
                "main.bronze.customers",
                "main.bronze.customers",
                0,
                "{\"id\":1}",
                "{\"after\":{\"id\":1}}",
                sourcePosition,
                Map.of("trace", "abc"),
                ZerobusOperation.UPDATE,
                "stable-key",
                1234L);
    }

    private static ZerobusChangeConsumerConfig configWith(String... options) {
        Configuration.Builder builder = Configuration.create()
                .with("endpoint", "unused.example.com")
                .with("workspace.url", "https://dbc.example.com")
                .with("client.id", "client")
                .with("client.secret", "secret")
                .with("payload.mode", "envelope");
        for (int index = 0; index < options.length; index += 2) {
            builder = builder.with(options[index], options[index + 1]);
        }
        return new ZerobusChangeConsumerConfig(builder.build());
    }

    @SuppressWarnings("unchecked")
    private static <P> ZerobusStreamHandle<P> mockStream() {
        return mock(ZerobusStreamHandle.class);
    }

    @SuppressWarnings("unchecked")
    private static ZerobusChangeConsumer consumer(ZerobusChangeConsumerConfig config,
                                                  Map<String, ZerobusStreamHandle<String>> jsonStreams,
                                                  Map<String, ZerobusStreamHandle<byte[]>> protobufStreams)
            throws Exception {
        ZerobusChangeConsumer consumer = new ZerobusChangeConsumer();
        set(consumer, "config", config);
        ((Map<String, ZerobusStreamHandle<String>>) get(consumer, "streams")).putAll(jsonStreams);
        ((Map<String, ZerobusStreamHandle<byte[]>>) get(consumer, "protobufStreams")).putAll(protobufStreams);
        return consumer;
    }

    private static void set(Object target, String field, Object value) throws Exception {
        Field declaredField = target.getClass().getDeclaredField(field);
        declaredField.setAccessible(true);
        declaredField.set(target, value);
    }

    private static Object get(Object target, String field) throws Exception {
        Field declaredField = target.getClass().getDeclaredField(field);
        declaredField.setAccessible(true);
        return declaredField.get(target);
    }

    @SuppressWarnings("unchecked")
    private static BatchEvent event(String destination, String key, String value, int partition) {
        BatchEvent event = mock(BatchEvent.class);
        when(event.destination()).thenReturn(destination);
        when(event.partition()).thenReturn(partition);
        when(event.key()).thenReturn(key);
        when(event.value()).thenReturn(value);
        when(event.headers()).thenReturn(new ArrayList<Header<Object>>());
        return event;
    }

    @SafeVarargs
    @SuppressWarnings("unchecked")
    private static CapturingEvents<BatchEvent> events(BatchEvent... events) {
        CapturingEvents<BatchEvent> batch = mock(CapturingEvents.class);
        when(batch.records()).thenReturn(List.of(events));
        return batch;
    }
}
