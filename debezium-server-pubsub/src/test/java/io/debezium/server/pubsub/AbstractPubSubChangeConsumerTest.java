/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pubsub;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.pubsub.v1.PubsubMessage;

import io.debezium.engine.Header;
import io.debezium.runtime.BatchEvent;

/**
 * Shared unit tests for the {@code PubSub} and {@code PubSub Lite} header to attribute mapping.
 *
 * <p>Both sinks share the raw-header extraction in {@code BaseChangeConsumer}; these tests verify
 * that each sink wires the resolved headers into the message attributes.
 */
abstract class AbstractPubSubChangeConsumerTest {

    @BeforeEach
    void setUp() {
        createConsumer();
        setHeaderSchemasEnabled(true);
    }

    @Test
    void testSchemasEnabledKeepsConverterSerializedHeaders() {
        String envelope = "{\"schema\":{\"type\":\"string\",\"optional\":false},\"payload\":\"00f067aa0ba902b7\"}";
        BatchEvent event = createEventWithHeaders(
                Map.of("id", "00f067aa0ba902b7"),
                Map.of("id", envelope));

        PubsubMessage message = buildPubSubMessage(event);

        assertThat(message.getAttributesMap()).containsEntry("id", envelope);
    }

    @Test
    void testSchemasDisabledUsesRawConnectHeaderValue() {
        setHeaderSchemasEnabled(false);

        // With debezium.format.header.schemas.enable=false the JsonConverter renders a string header
        // as a JSON string, i.e. wrapped in quotes; the raw Connect value must be used instead.
        BatchEvent event = createEventWithHeaders(
                Map.of("id", "00f067aa0ba902b7"),
                Map.of("id", "\"00f067aa0ba902b7\""));

        PubsubMessage message = buildPubSubMessage(event);

        assertThat(message.getAttributesMap()).containsEntry("id", "00f067aa0ba902b7");
        assertThat(message.getAttributesMap().get("id")).doesNotContain("\"");
    }

    @Test
    void testSchemasDisabledForwardsMultipleHeaders() {
        setHeaderSchemasEnabled(false);

        BatchEvent event = createEventWithHeaders(
                Map.of("eventType", "OrderCreated", "requestType", "command"),
                Map.of("eventType", "\"OrderCreated\"", "requestType", "\"command\""));

        PubsubMessage message = buildPubSubMessage(event);

        assertThat(message.getAttributesMap())
                .containsEntry("eventType", "OrderCreated")
                .containsEntry("requestType", "command");
    }

    /**
     * Creates the sink under test. Invoked before each test.
     */
    abstract void createConsumer();

    /**
     * Toggles the {@code debezium.format.header.schemas.enable} state on the sink under test.
     */
    abstract void setHeaderSchemasEnabled(boolean enabled);

    /**
     * Builds a Pub/Sub message from the given event using the sink under test.
     */
    abstract PubsubMessage buildPubSubMessage(BatchEvent event);

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
