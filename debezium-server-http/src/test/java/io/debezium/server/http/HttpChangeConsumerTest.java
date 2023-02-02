/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.Header;

public class HttpChangeConsumerTest {

    @Test
    public void verifyGenerateRequestWithDefaultConfig() throws URISyntaxException {
        HttpChangeConsumer changeConsumer = new HttpChangeConsumer();
        changeConsumer.initWithConfig(generateMockConfig(Map.of(
                HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_WEBHOOK_URL, "http://url",
                "debezium.format.value", "avro")));
        HttpRequest request = changeConsumer.generateRequest(createChangeEvent());

        String value = request.headers().firstValue("X-DEBEZIUM-h1key").orElse(null);
        assertEquals("aDFWYWx1ZQ==", value);
    }

    @Test
    public void verifyGenerateRequestWithBase64EncodingDisabled() throws URISyntaxException {
        HttpChangeConsumer changeConsumer = new HttpChangeConsumer();
        changeConsumer.initWithConfig(generateMockConfig(Map.of(
                HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_HEADERS_ENCODE_BASE64, false,
                HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_WEBHOOK_URL, "http://url",
                "debezium.format.value", "avro")));
        HttpRequest request = changeConsumer.generateRequest(createChangeEvent());

        String value = request.headers().firstValue("X-DEBEZIUM-h1key").orElse(null);
        assertEquals("h1Value", value);
    }

    @Test
    public void verifyGenerateRequestWithDifferentHeaderPrefix() throws URISyntaxException {
        HttpChangeConsumer changeConsumer = new HttpChangeConsumer();
        changeConsumer.initWithConfig(generateMockConfig(Map.of(
                HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_HEADERS_ENCODE_BASE64, false,
                HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_HEADERS_PREFIX, "XYZ-DBZ-",
                HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_WEBHOOK_URL, "http://url",
                "debezium.format.value", "avro")));
        HttpRequest request = changeConsumer.generateRequest(createChangeEvent());

        String value = request.headers().firstValue("XYZ-DBZ-h1key").orElse(null);
        assertEquals("h1Value", value);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static ChangeEvent<Object, Object> createChangeEvent() {

        ChangeEvent<Object, Object> result = mock(ChangeEvent.class);
        when(result.key()).thenReturn("key");
        when(result.value()).thenReturn("value");
        when(result.destination()).thenReturn("dest");
        Header header = mock(Header.class);
        when(header.getKey()).thenReturn("h1Key");
        when(header.getValue()).thenReturn("h1Value");
        when(result.headers()).thenReturn(List.of(header));
        return result;

    }

    private Config generateMockConfig(Map<String, Object> config) {
        Config result = mock(Config.class);

        for (Map.Entry<String, Object> entry : config.entrySet()) {
            Object value = entry.getValue();
            when(result.getValue(eq(entry.getKey()), any())).thenReturn(value);
            when(result.getOptionalValue(eq(entry.getKey()), any())).thenReturn(Optional.of(value));
        }

        return result;
    }
}
