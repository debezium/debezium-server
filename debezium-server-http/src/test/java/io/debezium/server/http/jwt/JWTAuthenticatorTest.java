/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http.jwt;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;
import java.util.UUID;

import javax.net.ssl.SSLSession;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.debezium.server.http.util.MutableClock;

public class JWTAuthenticatorTest {

    @Test
    public void unauthenticated_performs_initial_auth_and_sets_header() throws Exception {
        HttpClient client = mock(HttpClient.class);
        URI authURI = new URI("http://test.com/auth/authenticate");
        URI refreshURI = new URI("http://test.com/auth/refreshToken");
        Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        String initialAuthBody = "{\"jwt\":\"token-1\",\"jwt_refresh_token\":\"refresh-1\",\"expires_in\":10000}";
        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(mockResponse(200, initialAuthBody));

        JWTAuthenticator authenticator = new JWTAuthenticator(
                client,
                clock,
                authURI,
                refreshURI,
                "testUser",
                "testPassword",
                10000,
                10000,
                Duration.ofSeconds(10));

        // Act: perform authentication
        authenticator.authenticate();

        // Assert: correct endpoint was called
        ArgumentCaptor<HttpRequest> reqCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(client, times(1)).send(reqCaptor.capture(), any(HttpResponse.BodyHandler.class));
        assertEquals(authURI, reqCaptor.getValue().uri());

        // And header can be set
        HttpRequest.Builder builder = HttpRequest.newBuilder(new URI("http://test.com/endpoint"));
        authenticator.setAuthorizationHeader(builder, "", new UUID(0, 0));
        HttpHeaders headers = builder.build().headers();
        Optional<String> authValue = headers.firstValue("Authorization");
        assertTrue(authValue.isPresent());
        assertEquals("Bearer: token-1", authValue.get());
        assertEquals(1, headers.allValues("Authorization").size());
    }

    @Test
    public void expired_triggers_refresh_call() throws Exception {
        HttpClient client = mock(HttpClient.class);
        URI authURI = new URI("http://test.com/auth/authenticate");
        URI refreshURI = new URI("http://test.com/auth/refreshToken");

        String initialAuthBody = "{\"jwt\":\"token-1\",\"jwt_refresh_token\":\"refresh-1\",\"expires_in\":1000}";
        String refreshBody = "{\"jwt\":\"token-2\",\"jwt_refresh_token\":\"refresh-2\",\"expires_in\":1000}";
        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(
                mockResponse(200, initialAuthBody),
                mockResponse(200, refreshBody));

        Instant start = Instant.now();
        MutableClock testClock = new MutableClock(start, ZoneId.systemDefault());

        JWTAuthenticator authenticator = new JWTAuthenticator(
                client,
                testClock,
                authURI,
                refreshURI,
                "testUser",
                "testPassword",
                1000,
                1000,
                Duration.ofSeconds(10));

        // Act: initial authenticate
        authenticator.authenticate();

        // Advance time far into the future beyond expiration, then authenticate again to trigger refresh
        testClock.setNow(start.plus(Duration.ofDays(1)));
        authenticator.authenticate();

        // Assert: first call went to auth, second to refresh
        ArgumentCaptor<HttpRequest> reqCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(client, times(2)).send(reqCaptor.capture(), any(HttpResponse.BodyHandler.class));
        assertEquals(authURI, reqCaptor.getAllValues().get(0).uri());
        assertEquals(refreshURI, reqCaptor.getAllValues().get(1).uri());

        // And header now uses refreshed token
        HttpRequest.Builder builder = HttpRequest.newBuilder(new URI("http://test.com/endpoint"));
        authenticator.setAuthorizationHeader(builder, "", new UUID(0, 0));
        String header = builder.build().headers().firstValue("Authorization").orElse("");
        assertEquals("Bearer: token-2", header);
    }

    @Test
    public void active_does_not_reauthenticate_and_sets_header() throws Exception {
        HttpClient client = mock(HttpClient.class);
        URI authURI = new URI("http://test.com/auth/authenticate");
        URI refreshURI = new URI("http://test.com/auth/refreshToken");
        Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        String initialAuthBody = "{\"jwt\":\"token-1\",\"jwt_refresh_token\":\"refresh-1\",\"expires_in\":600000}"; // 10 minutes
        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(mockResponse(200, initialAuthBody));

        JWTAuthenticator authenticator = new JWTAuthenticator(
                client,
                clock,
                authURI,
                refreshURI,
                "testUser",
                "testPassword",
                600000,
                600000,
                Duration.ofSeconds(10));

        // Act: initial authenticate
        authenticator.authenticate();
        // Act: authenticate again while still active (no additional send should occur)
        authenticator.authenticate();

        // Assert: send called only once
        verify(client, times(1)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

        // Header is set with current token
        HttpRequest.Builder builder = HttpRequest.newBuilder(new URI("http://test.com/endpoint"));
        authenticator.setAuthorizationHeader(builder, "", new UUID(0, 0));
        String header = builder.build().headers().firstValue("Authorization").orElse("");
        assertEquals("Bearer: token-1", header);
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
            public java.util.Optional<SSLSession> sslSession() {
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
