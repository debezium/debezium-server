/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.List;
import java.util.Map;

import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.Header;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;

public class HttpChangeConsumerTest {

    @Test
    public void verifyGenerateRequestWithDefaultConfig() throws Exception {

        HttpChangeConsumer changeConsumer = createTestHttpChangeConsumer(Map.of(
                HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_WEBHOOK_URL, "http://url",
                "debezium.format.value", "avro"));
        HttpRequest request = changeConsumer.generateRequest(createChangeEvent()).build();

        String value = request.headers().firstValue("X-DEBEZIUM-h1key").orElse(null);
        assertEquals("aDFWYWx1ZQ==", value);
    }

    @Test
    public void verifyGenerateRequestWithBase64EncodingDisabled() throws Exception {
        HttpChangeConsumer changeConsumer = createTestHttpChangeConsumer(Map.of(
                HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_HEADERS_ENCODE_BASE64, "false",
                HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_WEBHOOK_URL, "http://url",
                "debezium.format.value", "avro"));
        HttpRequest request = changeConsumer.generateRequest(createChangeEvent()).build();

        String value = request.headers().firstValue("X-DEBEZIUM-h1key").orElse(null);
        assertEquals("h1Value", value);
    }

    @Test
    public void verifyGenerateRequestHasNoDuplicateHeaders() throws Exception {
        HttpChangeConsumer changeConsumer = createTestHttpChangeConsumer(Map.of(
                HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_HEADERS_ENCODE_BASE64, "false",
                HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_WEBHOOK_URL, "http://url",
                "debezium.format.value", "avro"));
        HttpRequest request = changeConsumer.generateRequest(createChangeEvent()).build();

        List<String> headers1 = request.headers().allValues("X-DEBEZIUM-h1key");
        assertEquals(1, headers1.size());

        HttpRequest request2 = changeConsumer.generateRequest(createChangeEvent()).build();

        List<String> headers2 = request2.headers().allValues("X-DEBEZIUM-h1key");
        assertEquals(1, headers2.size());
    }

    @Test
    public void verifyGenerateRequestWithDifferentHeaderPrefix() throws Exception {
        HttpChangeConsumer changeConsumer = createTestHttpChangeConsumer(Map.of(
                HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_HEADERS_ENCODE_BASE64, "false",
                HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_HEADERS_PREFIX, "XYZ-DBZ-",
                HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_WEBHOOK_URL, "http://url",
                "debezium.format.value", "avro"));
        HttpRequest request = changeConsumer.generateRequest(createChangeEvent()).build();

        String value = request.headers().firstValue("XYZ-DBZ-h1key").orElse(null);
        assertEquals("h1Value", value);
    }

    @Test
    public void testRecordSentWithIOExceptionNullMessage() throws Exception {
        HttpClient mockHttpClient = mock(HttpClient.class);
        when(mockHttpClient.send(any(), any())).thenThrow(new IOException());

        HttpChangeConsumer changeConsumer = createTestHttpChangeConsumer(
                Map.of(
                        HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_WEBHOOK_URL, "http://url",
                        HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_RETRIES, "3",
                        HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_RETRY_INTERVAL, "1",
                        "debezium.format.value", "json"),
                mockHttpClient);

        ChangeEvent<Object, Object> event = createChangeEvent();

        assertThrows(io.debezium.DebeziumException.class, () -> changeConsumer.handleBatch(List.of(event), mock()));
        verify(mockHttpClient, times(3)).send(any(), any());
    }

    @Test
    public void testRecordSentWithIOExceptionGoawayMessage() throws Exception {
        HttpClient mockHttpClient = mock(HttpClient.class);
        when(mockHttpClient.send(any(), any())).thenThrow(new IOException("HTTP/2 GOAWAY received"));

        HttpChangeConsumer changeConsumer = createTestHttpChangeConsumer(
                Map.of(
                        HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_WEBHOOK_URL, "http://url",
                        HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_RETRIES, "2",
                        HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_RETRY_INTERVAL, "1",
                        "debezium.format.value", "json"),
                mockHttpClient);

        ChangeEvent<Object, Object> event = createChangeEvent();

        // Should retry when GOAWAY is received and eventually throw DebeziumException
        assertThrows(io.debezium.DebeziumException.class, () -> changeConsumer.handleBatch(List.of(event), mock()));
        verify(mockHttpClient, times(2)).send(any(), any());
    }

    // Test subclass that allows injecting a mock HttpClient
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

    private HttpChangeConsumer createTestHttpChangeConsumer(Map<String, String> testValues) throws URISyntaxException, IOException {
        return createTestHttpChangeConsumer(testValues, mock(HttpClient.class));
    }

    private HttpChangeConsumer createTestHttpChangeConsumer(Map<String, String> testValues, HttpClient mockClient) throws URISyntaxException, IOException {
        // 'inject' HttpClient mock
        HttpChangeConsumer result = new HttpChangeConsumer() {
            @Override
            HttpClient createHttpClient() {
                return mockClient;
            }
        };
        Config testConfig = new SmallRyeConfigBuilder()
                .withSources(new PropertiesConfigSource(testValues, "test.properties"))
                .build();

        result.initWithConfig(testConfig);

        return result;
    }
}
