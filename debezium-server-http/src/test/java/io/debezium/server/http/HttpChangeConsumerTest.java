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

import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
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

    @Test
    public void testBatchModeSendsArrayPayload() throws Exception {
        HttpClient mockHttpClient = mock(HttpClient.class);
        @SuppressWarnings("unchecked")
        HttpResponse<String> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(200);
        doReturn(mockResponse).when(mockHttpClient).send(any(), any());

        HttpChangeConsumer changeConsumer = createTestHttpChangeConsumer(
                Map.of(
                        HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_WEBHOOK_URL, "http://url",
                        HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_BATCH_ENABLED, "true",
                        "debezium.format.value", "json"),
                mockHttpClient);

        ChangeEvent<Object, Object> event1 = createChangeEventWithValue("{\"id\":1}");
        ChangeEvent<Object, Object> event2 = createChangeEventWithValue("{\"id\":2}");

        @SuppressWarnings("unchecked")
        DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer = mock(DebeziumEngine.RecordCommitter.class);
        changeConsumer.handleBatch(List.of(event1, event2), committer);

        // Should send exactly one HTTP request (the batch)
        var reqCaptor = org.mockito.ArgumentCaptor.forClass(HttpRequest.class);
        verify(mockHttpClient, times(1)).send(reqCaptor.capture(), any());

        // Verify payload is a JSON array
        HttpRequest sentRequest = reqCaptor.getValue();
        assertTrue(sentRequest.bodyPublisher().isPresent());

        // Verify all records were marked processed
        verify(committer, times(2)).markProcessed(any());
        verify(committer, times(1)).markBatchFinished();
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
                        HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_WEBHOOK_URL, "http://url",
                        HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_BATCH_ENABLED, "true",
                        HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_BATCH_MAX_SIZE, "2",
                        "debezium.format.value", "json"),
                mockHttpClient);

        ChangeEvent<Object, Object> event1 = createChangeEventWithValue("{\"id\":1}");
        ChangeEvent<Object, Object> event2 = createChangeEventWithValue("{\"id\":2}");
        ChangeEvent<Object, Object> event3 = createChangeEventWithValue("{\"id\":3}");
        ChangeEvent<Object, Object> event4 = createChangeEventWithValue("{\"id\":4}");
        ChangeEvent<Object, Object> event5 = createChangeEventWithValue("{\"id\":5}");

        @SuppressWarnings("unchecked")
        DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer = mock(DebeziumEngine.RecordCommitter.class);
        changeConsumer.handleBatch(List.of(event1, event2, event3, event4, event5), committer);

        // 5 events with max-size=2 should produce 3 HTTP requests (2+2+1)
        verify(mockHttpClient, times(3)).send(any(), any());

        // All 5 records should be marked processed
        verify(committer, times(5)).markProcessed(any());
        verify(committer, times(1)).markBatchFinished();
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
                        HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_WEBHOOK_URL, "http://url",
                        "debezium.format.value", "json"),
                mockHttpClient);

        ChangeEvent<Object, Object> event1 = createChangeEventWithValue("{\"id\":1}");
        ChangeEvent<Object, Object> event2 = createChangeEventWithValue("{\"id\":2}");

        @SuppressWarnings("unchecked")
        DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer = mock(DebeziumEngine.RecordCommitter.class);
        changeConsumer.handleBatch(List.of(event1, event2), committer);

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
                        HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_WEBHOOK_URL, "http://url",
                        HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_BATCH_ENABLED, "true",
                        HttpChangeConsumer.PROP_PREFIX + HttpChangeConsumer.PROP_BATCH_MAX_SIZE, "2",
                        HttpChangeConsumer.PROP_AUTHENTICATION_PREFIX + HttpChangeConsumer.PROP_AUTHENTICATION_TYPE, "oauth2",
                        HttpChangeConsumer.PROP_AUTHENTICATION_PREFIX + "oauth2.client_id", "test-client",
                        HttpChangeConsumer.PROP_AUTHENTICATION_PREFIX + "oauth2.client_secret", "test-secret",
                        HttpChangeConsumer.PROP_AUTHENTICATION_PREFIX + "oauth2.token_url", "http://auth.example.com/token",
                        "debezium.format.value", "json"),
                mockHttpClient);

        ChangeEvent<Object, Object> event1 = createChangeEventWithValue("{\"id\":1}");
        ChangeEvent<Object, Object> event2 = createChangeEventWithValue("{\"id\":2}");
        ChangeEvent<Object, Object> event3 = createChangeEventWithValue("{\"id\":3}");

        @SuppressWarnings("unchecked")
        DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer = mock(DebeziumEngine.RecordCommitter.class);
        changeConsumer.handleBatch(List.of(event1, event2, event3), committer);

        // 1 token request + 2 batch sends (chunks of 2+1) = 3 total HTTP calls
        verify(mockHttpClient, times(3)).send(any(), any());

        // All records should be marked processed
        verify(committer, times(3)).markProcessed(any());
        verify(committer, times(1)).markBatchFinished();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static ChangeEvent<Object, Object> createChangeEventWithValue(String value) {
        ChangeEvent<Object, Object> result = mock(ChangeEvent.class);
        when(result.key()).thenReturn("key");
        when(result.value()).thenReturn(value);
        when(result.destination()).thenReturn("dest");
        when(result.headers()).thenReturn(List.of());
        return result;
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
