/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http.oauth2;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Flow;

import javax.net.ssl.SSLSession;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.debezium.DebeziumException;
import io.debezium.server.http.util.MutableClock;

public class OAuth2AuthenticatorTest {

    @Test
    public void default_post_with_client_secret_basic() throws Exception {
        HttpClient client = mock(HttpClient.class);
        URI tokenUri = new URI("https://api.example.com/oauth/token");
        Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        String tokenBody = "{\"access_token\":\"test-token-123\",\"expires_in\":3600,\"token_type\":\"bearer\"}";
        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse(200, tokenBody));

        OAuth2Authenticator authenticator = new OAuth2Authenticator(
                client, clock, tokenUri, "my-client-id", "my-client-secret", Duration.ofSeconds(30),
                null, Map.of(),
                OAuth2Authenticator.ClientAuthMethod.CLIENT_SECRET_BASIC,
                OAuth2Authenticator.TokenHttpMethod.POST);

        authenticator.authenticate();

        ArgumentCaptor<HttpRequest> reqCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(client, times(1)).send(reqCaptor.capture(), any(HttpResponse.BodyHandler.class));
        HttpRequest sentRequest = reqCaptor.getValue();

        // Should be POST
        assertEquals("POST", sentRequest.method());
        assertEquals(tokenUri, sentRequest.uri());

        // Should have Basic auth header
        String authHeader = sentRequest.headers().firstValue("Authorization").orElse("");
        assertEquals("Basic bXktY2xpZW50LWlkOm15LWNsaWVudC1zZWNyZXQ=", authHeader);

        // Should have form-urlencoded content type
        String contentType = sentRequest.headers().firstValue("Content-Type").orElse("");
        assertEquals("application/x-www-form-urlencoded", contentType);

        // Body should contain grant_type
        String body = extractBody(sentRequest);
        assertTrue(body.contains("grant_type=client_credentials"));
        // Body should NOT contain client_id/client_secret (those go in Basic header)
        assertTrue(!body.contains("client_id="));
        assertTrue(!body.contains("client_secret="));

        // Verify Bearer header is set on outgoing requests
        HttpRequest.Builder builder = HttpRequest.newBuilder(new URI("http://test.com/endpoint"));
        authenticator.setAuthorizationHeader(builder, "", UUID.randomUUID());
        String bearerHeader = builder.build().headers().firstValue("Authorization").orElse("");
        assertEquals("Bearer test-token-123", bearerHeader);
    }

    @Test
    public void post_with_client_secret_post() throws Exception {
        HttpClient client = mock(HttpClient.class);
        URI tokenUri = new URI("https://api.example.com/oauth/token");
        Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        String tokenBody = "{\"access_token\":\"token-post\",\"expires_in\":3600}";
        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse(200, tokenBody));

        OAuth2Authenticator authenticator = new OAuth2Authenticator(
                client, clock, tokenUri, "my-client", "my-secret", Duration.ofSeconds(30),
                null, Map.of(),
                OAuth2Authenticator.ClientAuthMethod.CLIENT_SECRET_POST,
                OAuth2Authenticator.TokenHttpMethod.POST);

        authenticator.authenticate();

        ArgumentCaptor<HttpRequest> reqCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(client, times(1)).send(reqCaptor.capture(), any(HttpResponse.BodyHandler.class));
        HttpRequest sentRequest = reqCaptor.getValue();

        // Should NOT have Basic auth header
        assertTrue(sentRequest.headers().firstValue("Authorization").isEmpty());

        // Body should contain client credentials
        String body = extractBody(sentRequest);
        assertTrue(body.contains("grant_type=client_credentials"));
        assertTrue(body.contains("client_id=my-client"));
        assertTrue(body.contains("client_secret=my-secret"));
    }

    @Test
    public void post_with_scope() throws Exception {
        HttpClient client = mock(HttpClient.class);
        URI tokenUri = new URI("https://api.example.com/oauth/token");
        Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        String tokenBody = "{\"access_token\":\"token-scoped\",\"expires_in\":3600}";
        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse(200, tokenBody));

        OAuth2Authenticator authenticator = new OAuth2Authenticator(
                client, clock, tokenUri, "client", "secret", Duration.ofSeconds(30),
                "data:read data:write", Map.of(),
                OAuth2Authenticator.ClientAuthMethod.CLIENT_SECRET_BASIC,
                OAuth2Authenticator.TokenHttpMethod.POST);

        authenticator.authenticate();

        ArgumentCaptor<HttpRequest> reqCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(client, times(1)).send(reqCaptor.capture(), any(HttpResponse.BodyHandler.class));

        String body = extractBody(reqCaptor.getValue());
        assertTrue(body.contains("grant_type=client_credentials"));
        // Scope should be URL-encoded (spaces become +)
        assertTrue(body.contains("scope=data%3Aread+data%3Awrite"));
    }

    @Test
    public void post_with_additional_params() throws Exception {
        HttpClient client = mock(HttpClient.class);
        URI tokenUri = new URI("https://api.example.com/oauth/token");
        Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        String tokenBody = "{\"access_token\":\"token-extra\",\"expires_in\":3600}";
        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse(200, tokenBody));

        OAuth2Authenticator authenticator = new OAuth2Authenticator(
                client, clock, tokenUri, "client", "secret", Duration.ofSeconds(30),
                null, Map.of("audience", "https://my-api.example.com"),
                OAuth2Authenticator.ClientAuthMethod.CLIENT_SECRET_BASIC,
                OAuth2Authenticator.TokenHttpMethod.POST);

        authenticator.authenticate();

        ArgumentCaptor<HttpRequest> reqCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(client, times(1)).send(reqCaptor.capture(), any(HttpResponse.BodyHandler.class));

        String body = extractBody(reqCaptor.getValue());
        assertTrue(body.contains("grant_type=client_credentials"));
        assertTrue(body.contains("audience=https%3A%2F%2Fmy-api.example.com"));
    }

    @Test
    public void get_method_uses_basic_auth_no_body() throws Exception {
        HttpClient client = mock(HttpClient.class);
        URI tokenUri = new URI("https://api.example.com/oauth/token?grant_type=client_credentials");
        Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        String tokenBody = "{\"access_token\":\"token-get\",\"expires_in\":3600}";
        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse(200, tokenBody));

        OAuth2Authenticator authenticator = new OAuth2Authenticator(
                client, clock, tokenUri, "my-client-id", "my-client-secret", Duration.ofSeconds(30),
                null, Map.of(),
                OAuth2Authenticator.ClientAuthMethod.CLIENT_SECRET_BASIC,
                OAuth2Authenticator.TokenHttpMethod.GET);

        authenticator.authenticate();

        ArgumentCaptor<HttpRequest> reqCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(client, times(1)).send(reqCaptor.capture(), any(HttpResponse.BodyHandler.class));
        HttpRequest sentRequest = reqCaptor.getValue();

        assertEquals("GET", sentRequest.method());
        String authHeader = sentRequest.headers().firstValue("Authorization").orElse("");
        assertEquals("Basic bXktY2xpZW50LWlkOm15LWNsaWVudC1zZWNyZXQ=", authHeader);

        // Verify Bearer header is set on outgoing requests
        HttpRequest.Builder builder = HttpRequest.newBuilder(new URI("http://test.com/endpoint"));
        authenticator.setAuthorizationHeader(builder, "", UUID.randomUUID());
        assertEquals("Bearer token-get", builder.build().headers().firstValue("Authorization").orElse(""));
    }

    @Test
    public void does_not_reauthenticate_while_token_is_active() throws Exception {
        HttpClient client = mock(HttpClient.class);
        URI tokenUri = new URI("https://api.example.com/oauth/token");
        Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        String tokenBody = "{\"access_token\":\"token-1\",\"expires_in\":3600}";
        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse(200, tokenBody));

        OAuth2Authenticator authenticator = new OAuth2Authenticator(
                client, clock, tokenUri, "client", "secret", Duration.ofSeconds(30),
                null, Map.of(),
                OAuth2Authenticator.ClientAuthMethod.CLIENT_SECRET_BASIC,
                OAuth2Authenticator.TokenHttpMethod.POST);

        authenticator.authenticate();
        authenticator.authenticate();

        verify(client, times(1)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    @Test
    public void refreshes_token_when_expired() throws Exception {
        HttpClient client = mock(HttpClient.class);
        URI tokenUri = new URI("https://api.example.com/oauth/token");

        String tokenBody1 = "{\"access_token\":\"token-1\",\"expires_in\":100}";
        String tokenBody2 = "{\"access_token\":\"token-2\",\"expires_in\":100}";
        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse(200, tokenBody1), mockResponse(200, tokenBody2));

        Instant start = Instant.now();
        MutableClock testClock = new MutableClock(start, ZoneId.systemDefault());

        OAuth2Authenticator authenticator = new OAuth2Authenticator(
                client, testClock, tokenUri, "client", "secret", Duration.ofSeconds(30),
                null, Map.of(),
                OAuth2Authenticator.ClientAuthMethod.CLIENT_SECRET_BASIC,
                OAuth2Authenticator.TokenHttpMethod.POST);

        authenticator.authenticate();

        // Advance past expiration
        testClock.setNow(start.plus(Duration.ofDays(1)));
        authenticator.authenticate();

        verify(client, times(2)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

        // Should use the refreshed token
        HttpRequest.Builder builder = HttpRequest.newBuilder(new URI("http://test.com/endpoint"));
        authenticator.setAuthorizationHeader(builder, "", UUID.randomUUID());
        String header = builder.build().headers().firstValue("Authorization").orElse("");
        assertEquals("Bearer token-2", header);
    }

    @Test
    public void throws_on_failed_token_request() throws Exception {
        HttpClient client = mock(HttpClient.class);
        URI tokenUri = new URI("https://api.example.com/oauth/token");
        Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse(401, "Unauthorized"));

        OAuth2Authenticator authenticator = new OAuth2Authenticator(
                client, clock, tokenUri, "bad-client", "bad-secret", Duration.ofSeconds(30),
                null, Map.of(),
                OAuth2Authenticator.ClientAuthMethod.CLIENT_SECRET_BASIC,
                OAuth2Authenticator.TokenHttpMethod.POST);

        assertThrows(DebeziumException.class, authenticator::authenticate);
    }

    @Test
    public void throws_on_null_access_token() throws Exception {
        HttpClient client = mock(HttpClient.class);
        URI tokenUri = new URI("https://api.example.com/oauth/token");
        Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        String tokenBody = "{\"expires_in\":3600}";
        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse(200, tokenBody));

        OAuth2Authenticator authenticator = new OAuth2Authenticator(
                client, clock, tokenUri, "client", "secret", Duration.ofSeconds(30),
                null, Map.of(),
                OAuth2Authenticator.ClientAuthMethod.CLIENT_SECRET_BASIC,
                OAuth2Authenticator.TokenHttpMethod.POST);

        assertThrows(DebeziumException.class, authenticator::authenticate);
    }

    @Test
    public void throws_on_empty_access_token() throws Exception {
        HttpClient client = mock(HttpClient.class);
        URI tokenUri = new URI("https://api.example.com/oauth/token");
        Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        String tokenBody = "{\"access_token\":\"\",\"expires_in\":3600}";
        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse(200, tokenBody));

        OAuth2Authenticator authenticator = new OAuth2Authenticator(
                client, clock, tokenUri, "client", "secret", Duration.ofSeconds(30),
                null, Map.of(),
                OAuth2Authenticator.ClientAuthMethod.CLIENT_SECRET_BASIC,
                OAuth2Authenticator.TokenHttpMethod.POST);

        assertThrows(DebeziumException.class, authenticator::authenticate);
    }

    @Test
    public void throws_if_header_set_before_authenticate() throws Exception {
        URI tokenUri = new URI("https://api.example.com/oauth/token");
        Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        OAuth2Authenticator authenticator = new OAuth2Authenticator(
                mock(HttpClient.class), clock, tokenUri, "client", "secret", Duration.ofSeconds(30),
                null, Map.of(),
                OAuth2Authenticator.ClientAuthMethod.CLIENT_SECRET_BASIC,
                OAuth2Authenticator.TokenHttpMethod.POST);

        HttpRequest.Builder builder = HttpRequest.newBuilder(new URI("http://test.com/endpoint"));
        assertThrows(DebeziumException.class, () -> authenticator.setAuthorizationHeader(builder, "", UUID.randomUUID()));
    }

    /**
     * Extracts the body string from an HttpRequest by subscribing to its BodyPublisher.
     */
    private static String extractBody(HttpRequest request) {
        return request.bodyPublisher().map(publisher -> {
            List<ByteBuffer> buffers = new ArrayList<>();
            publisher.subscribe(new Flow.Subscriber<>() {
                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(ByteBuffer item) {
                    buffers.add(item);
                }

                @Override
                public void onError(Throwable throwable) {
                }

                @Override
                public void onComplete() {
                }
            });
            int totalSize = buffers.stream().mapToInt(ByteBuffer::remaining).sum();
            byte[] bytes = new byte[totalSize];
            int offset = 0;
            for (ByteBuffer buf : buffers) {
                int len = buf.remaining();
                buf.get(bytes, offset, len);
                offset += len;
            }
            return new String(bytes);
        }).orElse("");
    }

    private static HttpResponse<String> mockResponse(int status, String body) {
        return new HttpResponse<>() {
            @Override
            public int statusCode() {
                return status;
            }

            @Override
            public HttpRequest request() {
                return null;
            }

            @Override
            public Optional<HttpResponse<String>> previousResponse() {
                return Optional.empty();
            }

            @Override
            public HttpHeaders headers() {
                return HttpHeaders.of(emptyMap(), (a, b) -> true);
            }

            @Override
            public String body() {
                return body;
            }

            @Override
            public Optional<SSLSession> sslSession() {
                return Optional.empty();
            }

            @Override
            public URI uri() {
                return null;
            }

            @Override
            public HttpClient.Version version() {
                return null;
            }
        };
    }
}
