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

import io.debezium.config.Configuration;
import io.debezium.engine.Header;
import io.debezium.runtime.BatchEvent;

/**
 * Unit tests for the {@link PubSubLiteChangeConsumer} header to attribute mapping.
 *
 * <p>See <a href="https://github.com/debezium/dbz/issues/2783">DBZ issue #2783</a>.
 */
class PubSubLiteChangeConsumerTest {

    private PubSubLiteChangeConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new PubSubLiteChangeConsumer();
        consumer.config = new PubSubLiteChangeConsumerConfig(Configuration.create()
                .with(PubSubLiteChangeConsumerConfig.ORDERING_ENABLED, false)
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
