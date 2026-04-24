/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;

import io.debezium.engine.Header;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;

public class HttpChangeConsumerTest {

    @Test
    public void verifyGenerateRequestWithDefaultConfig() throws Exception {

        HttpChangeConsumer changeConsumer = createTestHttpChangeConsumer(Map.of(
                "debezium.sink.http.url", "http://url",
                "debezium.format.value", "avro"));
        HttpRequest request = changeConsumer.generateRequest(createBatchEvent()).build();

        String value = request.headers().firstValue("X-DEBEZIUM-h1key").orElse(null);
        assertEquals("aDFWYWx1ZQ==", value);
    }

    @Test
    public void verifyGenerateRequestWithBase64EncodingDisabled() throws Exception {
        HttpChangeConsumer changeConsumer = createTestHttpChangeConsumer(Map.of(
                "debezium.sink.http.headers.encode.base64", "false",
                "debezium.sink.http.url", "http://url",
                "debezium.format.value", "avro"));
        HttpRequest request = changeConsumer.generateRequest(createBatchEvent()).build();

        String value = request.headers().firstValue("X-DEBEZIUM-h1key").orElse(null);
        assertEquals("h1Value", value);
    }

    @Test
    public void verifyGenerateRequestHasNoDuplicateHeaders() throws Exception {
        HttpChangeConsumer changeConsumer = createTestHttpChangeConsumer(Map.of(
                "debezium.sink.http.headers.encode.base64", "false",
                "debezium.sink.http.url", "http://url",
                "debezium.format.value", "avro"));
        HttpRequest request = changeConsumer.generateRequest(createBatchEvent()).build();

        List<String> headers1 = request.headers().allValues("X-DEBEZIUM-h1key");
        assertEquals(1, headers1.size());

        HttpRequest request2 = changeConsumer.generateRequest(createBatchEvent()).build();

        List<String> headers2 = request2.headers().allValues("X-DEBEZIUM-h1key");
        assertEquals(1, headers2.size());
    }

    @Test
    public void verifyGenerateRequestWithDifferentHeaderPrefix() throws Exception {
        HttpChangeConsumer changeConsumer = createTestHttpChangeConsumer(Map.of(
                "debezium.sink.http.headers.encode.base64", "false",
                "debezium.sink.http.headers.prefix", "XYZ-DBZ-",
                "debezium.sink.http.url", "http://url",
                "debezium.format.value", "avro"));
        HttpRequest request = changeConsumer.generateRequest(createBatchEvent()).build();

        String value = request.headers().firstValue("XYZ-DBZ-h1key").orElse(null);
        assertEquals("h1Value", value);
    }

    @Test
    public void testRecordSentWithIOExceptionNullMessage() throws Exception {
        HttpClient mockHttpClient = mock(HttpClient.class);
        when(mockHttpClient.send(any(), any())).thenThrow(new IOException());

        HttpChangeConsumer changeConsumer = createTestHttpChangeConsumer(
                Map.of(
                        "debezium.sink.http.url", "http://url",
                        "debezium.sink.http.retries", "3",
                        "debezium.sink.http.retry.interval.ms", "1",
                        "debezium.format.value", "json"),
                mockHttpClient);

        var event = createBatchEvent();

        assertThrows(io.debezium.DebeziumException.class, () -> changeConsumer.handle(capturingEvents(event)));
        verify(mockHttpClient, times(3)).send(any(), any());
    }

    @Test
    public void testRecordSentWithIOExceptionGoawayMessage() throws Exception {
        HttpClient mockHttpClient = mock(HttpClient.class);
        when(mockHttpClient.send(any(), any())).thenThrow(new IOException("HTTP/2 GOAWAY received"));

        HttpChangeConsumer changeConsumer = createTestHttpChangeConsumer(
                Map.of(
                        "debezium.sink.http.url", "http://url",
                        "debezium.sink.http.retries", "2",
                        "debezium.sink.http.retry.interval.ms", "1",
                        "debezium.format.value", "json"),
                mockHttpClient);

        var event = createBatchEvent();

        // Should retry when GOAWAY is received and eventually throw DebeziumException
        assertThrows(io.debezium.DebeziumException.class, () -> changeConsumer.handle(capturingEvents(event)));
        verify(mockHttpClient, times(2)).send(any(), any());
    }

    @Test
    public void testBatchModeSendsArrayPayload() throws Exception {
        HttpClient mockHttpClient = mock(HttpClient.class);
        @SuppressWarnings("unchecked")
        HttpResponse<String> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(200);
        doReturn(mockResponse).when(mockHttpClient).send(any(), any());

        HttpChangeConsumer changeConsumer = createTestHttpChangeConsumer(
                Map.of(
                        "debezium.sink.http.url", "http://url",
                        "debezium.sink.http.batch.enabled", "true",
                        "debezium.format.value", "json"),
                mockHttpClient);

        var event1 = createChangeEventWithValue("{\"id\":1}");
        var event2 = createChangeEventWithValue("{\"id\":2}");

        changeConsumer.handle(capturingEvents(event1, event2));

        // Should send exactly one HTTP request (the batch)
        var reqCaptor = org.mockito.ArgumentCaptor.forClass(HttpRequest.class);
        verify(mockHttpClient, times(1)).send(reqCaptor.capture(), any());

        // Verify payload is a JSON array
        HttpRequest sentRequest = reqCaptor.getValue();
        assertTrue(sentRequest.bodyPublisher().isPresent());

        // Verify all records were marked processed
        verify(event1, times(1)).commit();
        verify(event2, times(1)).commit();
    }

    @Test
    public void testBatchModeChunksByMaxSize() throws Exception {
        HttpClient mockHttpClient = mock(HttpClient.class);
        @SuppressWarnings("unchecked")
        HttpResponse<String> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(200);
        doReturn(mockResponse).when(mockHttpClient).send(any(), any());

        HttpChangeConsumer changeConsumer = createTestHttpChangeConsumer(
                Map.of(
                        "debezium.sink.http.url", "http://url",
                        "debezium.sink.http.batch.enabled", "true",
                        "debezium.sink.http.batch.max-size", "2",
                        "debezium.format.value", "json"),
                mockHttpClient);

        var event1 = createChangeEventWithValue("{\"id\":1}");
        var event2 = createChangeEventWithValue("{\"id\":2}");
        var event3 = createChangeEventWithValue("{\"id\":3}");
        var event4 = createChangeEventWithValue("{\"id\":4}");
        var event5 = createChangeEventWithValue("{\"id\":5}");

        changeConsumer.handle(capturingEvents(event1, event2, event3, event4, event5));

        // 5 events with max-size=2 should produce 3 HTTP requests (2+2+1)
        verify(mockHttpClient, times(3)).send(any(), any());

        // All 5 records should be marked processed

        verify(event1, times(1)).commit();
        verify(event2, times(1)).commit();
        verify(event3, times(1)).commit();
        verify(event4, times(1)).commit();
        verify(event5, times(1)).commit();
    }

    @Test
    public void testBatchModeDisabledSendsIndividually() throws Exception {
        HttpClient mockHttpClient = mock(HttpClient.class);
        @SuppressWarnings("unchecked")
        HttpResponse<String> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(200);
        doReturn(mockResponse).when(mockHttpClient).send(any(), any());

        HttpChangeConsumer changeConsumer = createTestHttpChangeConsumer(
                Map.of(
                        "debezium.sink.http.url", "http://url",
                        "debezium.format.value", "json"),
                mockHttpClient);

        var event1 = createChangeEventWithValue("{\"id\":1}");
        var event2 = createChangeEventWithValue("{\"id\":2}");

        changeConsumer.handle(capturingEvents(event1, event2));

        // Should send two individual HTTP requests
        verify(mockHttpClient, times(2)).send(any(), any());
    }

    @Test
    public void testBatchModeWithAuthentication() throws Exception {
        HttpClient mockHttpClient = mock(HttpClient.class);

        // First call is the OAuth2 token request, subsequent calls are batch sends
        @SuppressWarnings("unchecked")
        HttpResponse<String> tokenResponse = mock(HttpResponse.class);
        when(tokenResponse.statusCode()).thenReturn(200);
        when(tokenResponse.body()).thenReturn("{\"access_token\":\"test-token\",\"expires_in\":3600,\"token_type\":\"bearer\"}");

        @SuppressWarnings("unchecked")
        HttpResponse<String> batchResponse = mock(HttpResponse.class);
        when(batchResponse.statusCode()).thenReturn(200);

        doReturn(tokenResponse, batchResponse, batchResponse)
                .when(mockHttpClient).send(any(), any());

        // Configure batch mode with OAuth2 authentication
        HttpChangeConsumer changeConsumer = createTestHttpChangeConsumer(
                Map.of(
                        "debezium.sink.http.url", "http://url",
                        "debezium.sink.http.batch.enabled", "true",
                        "debezium.sink.http.batch.max-size", "2",
                        "debezium.sink.http.authentication.type", "oauth2",
                        "debezium.sink.http.authentication.oauth2.client-id", "test-client",
                        "debezium.sink.http.authentication.oauth2.client-secret", "test-secret",
                        "debezium.sink.http.authentication.oauth2.token-url", "http://auth.example.com/token",
                        "debezium.format.value", "json"),
                mockHttpClient);

        var event1 = createChangeEventWithValue("{\"id\":1}");
        var event2 = createChangeEventWithValue("{\"id\":2}");
        var event3 = createChangeEventWithValue("{\"id\":3}");

        changeConsumer.handle(capturingEvents(event1, event2, event3));

        // 1 token request + 2 batch sends (chunks of 2+1) = 3 total HTTP calls
        verify(mockHttpClient, times(3)).send(any(), any());

        // All records should be marked processed
        verify(event1, times(1)).commit();
        verify(event2, times(1)).commit();
        verify(event3, times(1)).commit();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static BatchEvent createChangeEventWithValue(String value) {
        BatchEvent result = mock(BatchEvent.class);
        when(result.key()).thenReturn("key");
        when(result.value()).thenReturn(value);

        when(result.headers()).thenReturn(List.of());
        return result;
    }

    // Test subclass that allows injecting a mock HttpClient

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static BatchEvent createBatchEvent() {

        BatchEvent result = mock(BatchEvent.class);
        when(result.key()).thenReturn("key");
        when(result.value()).thenReturn("value");
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

    private static CapturingEvents<BatchEvent> capturingEvents(BatchEvent ...events) {
        return new CapturingEvents<>() {
            @Override
            public List<BatchEvent> records() {
                return List.of(events);
            }

            @Override
            public String destination() {
                return "dest";
            }

            @Override
            public String source() {
                return "";
            }

            @Override
            public String engine() {
                return "default";
            }
        };
    }
}
