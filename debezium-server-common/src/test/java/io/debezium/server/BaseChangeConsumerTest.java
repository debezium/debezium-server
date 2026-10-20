/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;

import io.debezium.engine.Header;
import io.debezium.runtime.BatchEvent;

/**
 * Unit tests for the header conversion helpers of {@link BaseChangeConsumer}.
 */
class BaseChangeConsumerTest {

    private static final String HEADER_SCHEMAS_ENABLE = "debezium.format.header.schemas.enable";
    private static final String SCHEMAS_ENABLE = "debezium.format.schemas.enable";

    private final BaseChangeConsumer consumer = new BaseChangeConsumer();

    @Test
    void testHeaderSchemasDefaultToEnabled() {
        assertThat(BaseChangeConsumer.isHeaderSchemasEnabled(mockConfig(Optional.empty(), Optional.empty()))).isTrue();
    }

    @Test
    void testHeaderSchemasDisabled() {
        assertThat(BaseChangeConsumer.isHeaderSchemasEnabled(mockConfig(Optional.of(false), Optional.empty()))).isFalse();
    }

    @Test
    void testHeaderSchemasEnabled() {
        assertThat(BaseChangeConsumer.isHeaderSchemasEnabled(mockConfig(Optional.of(true), Optional.empty()))).isTrue();
    }

    @Test
    void testHeaderSchemasFallBackToGlobalSetting() {
        // debezium.format.schemas.enable also disables schemas for the header converter
        assertThat(BaseChangeConsumer.isHeaderSchemasEnabled(mockConfig(Optional.empty(), Optional.of(false)))).isFalse();
    }

    @Test
    void testHeaderSchemasSettingTakesPrecedenceOverGlobalSetting() {
        assertThat(BaseChangeConsumer.isHeaderSchemasEnabled(mockConfig(Optional.of(true), Optional.of(false)))).isTrue();
        assertThat(BaseChangeConsumer.isHeaderSchemasEnabled(mockConfig(Optional.of(false), Optional.of(true)))).isFalse();
    }

    @Test
    void testResolveHeadersWithSchemasEnabledKeepsConverterSerializedHeaders() {
        String envelope = "{\"schema\":{\"type\":\"string\",\"optional\":false},\"payload\":\"00f067aa0ba902b7\"}";
        BatchEvent event = createEventWithHeaders(
                Map.of("id", "00f067aa0ba902b7"),
                Map.of("id", envelope));

        assertThat(consumer.resolveHeaders(event, true)).containsEntry("id", envelope);
    }

    @Test
    void testResolveHeadersWithSchemasDisabledUsesRawConnectHeaderValue() {
        // With debezium.format.header.schemas.enable=false the JsonConverter renders a string header
        // as a JSON string, i.e. wrapped in quotes; the raw Connect value must be used instead.
        BatchEvent event = createEventWithHeaders(
                Map.of("id", "00f067aa0ba902b7"),
                Map.of("id", "\"00f067aa0ba902b7\""));

        Map<String, String> headers = consumer.resolveHeaders(event, false);

        assertThat(headers).containsEntry("id", "00f067aa0ba902b7");
        assertThat(headers.get("id")).doesNotContain("\"");
    }

    @Test
    void testResolveHeadersWithSchemasDisabledForwardsMultipleHeaders() {
        BatchEvent event = createEventWithHeaders(
                Map.of("eventType", "OrderCreated", "requestType", "command"),
                Map.of("eventType", "\"OrderCreated\"", "requestType", "\"command\""));

        assertThat(consumer.resolveHeaders(event, false))
                .containsEntry("eventType", "OrderCreated")
                .containsEntry("requestType", "command");
    }

    @Test
    void testConvertRawHeadersHandlesByteArrayHeader() {
        ConnectHeaders headers = new ConnectHeaders();
        headers.addBytes("rawByteHeader", "hello".getBytes(StandardCharsets.UTF_8));

        assertThat(consumer.convertRawHeaders(createEventWithRawHeaders(headers, Map.of())))
                .containsEntry("rawByteHeader", "hello");
    }

    @Test
    void testConvertRawHeadersSkipsNullHeaderValue() {
        org.apache.kafka.connect.header.Header nullHeader = mock(org.apache.kafka.connect.header.Header.class);
        when(nullHeader.key()).thenReturn("nullHeader");
        when(nullHeader.value()).thenReturn(null);

        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("id", "00f067aa0ba902b7");
        headers.add(nullHeader);

        assertThat(consumer.convertRawHeaders(createEventWithRawHeaders(headers, Map.of())))
                .containsEntry("id", "00f067aa0ba902b7")
                .doesNotContainKey("nullHeader");
    }

    @Test
    void testConvertRawHeadersRendersLogicalDateHeaderWithToString() {
        // Non-primitive values are rendered with Object.toString(), the long-standing behavior of the
        // SNS sink; this test pins the representation down.
        java.util.Date date = new java.util.Date(0L);
        ConnectHeaders headers = new ConnectHeaders();
        headers.add("dateHeader", date, null);

        assertThat(consumer.convertRawHeaders(createEventWithRawHeaders(headers, Map.of())))
                .containsEntry("dateHeader", date.toString());
    }

    @Test
    void testConvertRawHeadersRendersStructHeaderWithToString() {
        org.apache.kafka.connect.data.Schema structSchema = org.apache.kafka.connect.data.SchemaBuilder.struct()
                .field("field", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                .build();
        org.apache.kafka.connect.data.Struct struct = new org.apache.kafka.connect.data.Struct(structSchema).put("field", "value");

        ConnectHeaders headers = new ConnectHeaders();
        headers.add("structHeader", struct, structSchema);

        assertThat(consumer.convertRawHeaders(createEventWithRawHeaders(headers, Map.of())))
                .containsEntry("structHeader", struct.toString());
    }

    private static Config mockConfig(Optional<Boolean> headerSchemasEnabled, Optional<Boolean> schemasEnabled) {
        Config config = mock(Config.class);
        when(config.getOptionalValue(HEADER_SCHEMAS_ENABLE, Boolean.class)).thenReturn(headerSchemasEnabled);
        when(config.getOptionalValue(SCHEMAS_ENABLE, Boolean.class)).thenReturn(schemasEnabled);
        return config;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static BatchEvent createEventWithHeaders(Map<String, String> connectHeaders,
                                                     Map<String, String> converterHeaders) {
        ConnectHeaders headers = new ConnectHeaders();
        connectHeaders.forEach(headers::addString);
        return createEventWithRawHeaders(headers, converterHeaders);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static BatchEvent createEventWithRawHeaders(ConnectHeaders headers,
                                                        Map<String, String> converterHeaders) {
        SourceRecord sourceRecord = mock(SourceRecord.class);
        when(sourceRecord.headers()).thenReturn(headers);
        when(sourceRecord.key()).thenReturn("key");
        when(sourceRecord.value()).thenReturn("{\"id\":1}");

        List<Header<Object>> serializedHeaders = new ArrayList<>();
        converterHeaders.forEach((key, value) -> {
            Header<Object> header = mock(Header.class);
            when(header.getKey()).thenReturn(key);
            when(header.getValue()).thenReturn(value);
            serializedHeaders.add(header);
        });

        BatchEvent event = mock(BatchEvent.class);
        when(event.key()).thenReturn("key");
        when(event.value()).thenReturn("{\"id\":1}");
        when(event.destination()).thenReturn("testc.inventory.customers");
        when(event.record()).thenReturn(sourceRecord);
        when(event.headers()).thenReturn(serializedHeaders);
        return event;
    }
}
