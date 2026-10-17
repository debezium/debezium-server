/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pubsub;

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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.pubsub.v1.PubsubMessage;

import io.debezium.config.Configuration;
import io.debezium.engine.Header;
import io.debezium.runtime.BatchEvent;

/**
 * Unit tests for the {@link PubSubChangeConsumer} header to attribute mapping.
 *
 * <p>See <a href="https://github.com/debezium/dbz/issues/2783">DBZ issue #2783</a>.
 */
class PubSubChangeConsumerTest {

    private static final String HEADER_SCHEMAS_ENABLE = "debezium.format.header.schemas.enable";

    private PubSubChangeConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new PubSubChangeConsumer();
        consumer.config = new PubSubChangeConsumerConfig(Configuration.create()
                .with(PubSubChangeConsumerConfig.ORDERING_ENABLED, false)
                .build());
        consumer.headerSchemasEnabled = true;
    }

    @Test
    void testSchemasEnabledKeepsConverterSerializedHeaders() {
        String envelope = "{\"schema\":{\"type\":\"string\",\"optional\":false},\"payload\":\"00f067aa0ba902b7\"}";
        BatchEvent event = createEventWithHeaders(
                Map.of("id", "00f067aa0ba902b7"),
                Map.of("id", envelope));

        PubsubMessage message = consumer.buildPubSubMessage(event);

        assertThat(message.getAttributesMap()).containsEntry("id", envelope);
    }

    @Test
    void testSchemasDisabledUsesRawConnectHeaderValue() {
        consumer.headerSchemasEnabled = false;

        // With debezium.format.header.schemas.enable=false the JsonConverter renders a string header
        // as a JSON string, i.e. wrapped in quotes; the raw Connect value must be used instead.
        BatchEvent event = createEventWithHeaders(
                Map.of("id", "00f067aa0ba902b7"),
                Map.of("id", "\"00f067aa0ba902b7\""));

        PubsubMessage message = consumer.buildPubSubMessage(event);

        assertThat(message.getAttributesMap()).containsEntry("id", "00f067aa0ba902b7");
        assertThat(message.getAttributesMap().get("id")).doesNotContain("\"");
    }

    @Test
    void testSchemasDisabledForwardsMultipleHeaders() {
        consumer.headerSchemasEnabled = false;

        BatchEvent event = createEventWithHeaders(
                Map.of("eventType", "OrderCreated", "requestType", "command"),
                Map.of("eventType", "\"OrderCreated\"", "requestType", "\"command\""));

        PubsubMessage message = consumer.buildPubSubMessage(event);

        assertThat(message.getAttributesMap())
                .containsEntry("eventType", "OrderCreated")
                .containsEntry("requestType", "command");
    }

    @Test
    void testSchemasDisabledHandlesByteArrayHeader() {
        consumer.headerSchemasEnabled = false;

        ConnectHeaders headers = new ConnectHeaders();
        headers.addBytes("rawByteHeader", "hello".getBytes(StandardCharsets.UTF_8));

        PubsubMessage message = consumer.buildPubSubMessage(createEventWithRawHeaders(headers));

        assertThat(message.getAttributesMap()).containsEntry("rawByteHeader", "hello");
    }

    @Test
    void testSchemasDisabledSkipsNullHeaderValues() {
        consumer.headerSchemasEnabled = false;

        org.apache.kafka.connect.header.Header nullHeader = mock(org.apache.kafka.connect.header.Header.class);
        when(nullHeader.key()).thenReturn("nullHeader");
        when(nullHeader.value()).thenReturn(null);

        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("id", "00f067aa0ba902b7");
        headers.add(nullHeader);

        PubsubMessage message = consumer.buildPubSubMessage(createEventWithRawHeaders(headers));

        assertThat(message.getAttributesMap())
                .containsOnlyKeys("id")
                .containsEntry("id", "00f067aa0ba902b7");
    }

    @Test
    void testSchemasDisabledHandlesMissingSourceRecord() {
        consumer.headerSchemasEnabled = false;

        BatchEvent event = mock(BatchEvent.class);
        when(event.key()).thenReturn("key");
        when(event.value()).thenReturn("{\"id\":1}");
        when(event.destination()).thenReturn("testc.inventory.customers");
        when(event.record()).thenReturn(null);
        when(event.headers()).thenReturn(List.of());

        PubsubMessage message = consumer.buildPubSubMessage(event);

        assertThat(message.getAttributesMap()).isEmpty();
    }

    @Test
    void testHeaderSchemasDefaultToEnabled() {
        assertThat(HeaderSchemasProbe.headerSchemasEnabled(mockConfig(Optional.empty()))).isTrue();
    }

    @Test
    void testHeaderSchemasDisabled() {
        assertThat(HeaderSchemasProbe.headerSchemasEnabled(mockConfig(Optional.of(false)))).isFalse();
    }

    @Test
    void testHeaderSchemasEnabled() {
        assertThat(HeaderSchemasProbe.headerSchemasEnabled(mockConfig(Optional.of(true)))).isTrue();
    }

    private static Config mockConfig(Optional<Boolean> headerSchemasEnabled) {
        Config config = mock(Config.class);
        when(config.getOptionalValue(HEADER_SCHEMAS_ENABLE, Boolean.class)).thenReturn(headerSchemasEnabled);
        return config;
    }

    /** Exposes the inherited {@code isHeaderSchemasEnabled} helper, which is not reachable from this package. */
    private static class HeaderSchemasProbe extends PubSubChangeConsumer {
        static boolean headerSchemasEnabled(Config config) {
            return PubSubChangeConsumer.isHeaderSchemasEnabled(config);
        }
    }

    private static BatchEvent createEventWithHeaders(Map<String, String> connectHeaders,
                                                     Map<String, String> converterHeaders) {
        ConnectHeaders headers = new ConnectHeaders();
        connectHeaders.forEach(headers::addString);
        return createEventWithRawHeaders(headers, converterHeaders);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static BatchEvent createEventWithRawHeaders(ConnectHeaders headers) {
        return createEventWithRawHeaders(headers, Map.of());
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
