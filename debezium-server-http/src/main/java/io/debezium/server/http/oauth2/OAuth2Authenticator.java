/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http.oauth2;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.server.http.Authenticator;
import io.debezium.server.http.HttpUtil;

/**
 * Implements OAuth2 client_credentials grant authentication (RFC 6749 Section 4.4).
 *
 * <p>Supports two client authentication methods:</p>
 * <ul>
 *   <li>{@code client_secret_basic} (default) — sends client_id:client_secret as an HTTP Basic Authorization header</li>
 *   <li>{@code client_secret_post} — sends client_id and client_secret in the POST request body</li>
 * </ul>
 *
 * <p>By default, sends a standard POST request with {@code application/x-www-form-urlencoded} body
 * containing {@code grant_type=client_credentials}. An optional {@code scope} parameter and
 * additional custom parameters can be included. For non-standard OAuth2 servers that require GET,
 * the HTTP method can be overridden.</p>
 */
public class OAuth2Authenticator implements Authenticator {
    private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2Authenticator.class);

    private static final double EXPIRATION_BUFFER_MULTIPLIER = 0.9;

    /**
     * Client authentication methods per RFC 6749.
     */
    public enum ClientAuthMethod {
        /** Send credentials as HTTP Basic Authorization header (RFC 6749 Section 2.3.1). */
        CLIENT_SECRET_BASIC,
        /** Send credentials as form fields in the POST body. */
        CLIENT_SECRET_POST
    }

    /**
     * HTTP method for the token request.
     */
    public enum TokenHttpMethod {
        /** Standard per RFC 6749 Section 4.4.2. */
        POST,
        /** Non-standard; some providers accept GET with query parameters. */
        GET
    }

    private final HttpClient client;
    private final Clock clock;
    private final URI tokenUri;
    private final String clientId;
    private final String clientSecret;
    private final Duration httpTimeout;
    private final String scope;
    private final Map<String, String> additionalParams;
    private final ClientAuthMethod clientAuthMethod;
    private final TokenHttpMethod tokenHttpMethod;
    private final ObjectMapper mapper = new ObjectMapper();

    private String accessToken;
    private Instant tokenExpiresAt;

    OAuth2Authenticator(HttpClient client, Clock clock, URI tokenUri,
                        String clientId, String clientSecret, Duration httpTimeout,
                        String scope, Map<String, String> additionalParams,
                        ClientAuthMethod clientAuthMethod, TokenHttpMethod tokenHttpMethod) {
        this.client = client;
        this.clock = clock;
        this.tokenUri = tokenUri;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.httpTimeout = httpTimeout;
        this.scope = scope;
        this.additionalParams = additionalParams;
        this.clientAuthMethod = clientAuthMethod;
        this.tokenHttpMethod = tokenHttpMethod;
        this.tokenExpiresAt = Instant.EPOCH;
    }

    @Override
    public synchronized void authenticate() throws InterruptedException {
        if (accessToken != null && Instant.now(clock).isBefore(tokenExpiresAt)) {
            return;
        }

        LOGGER.info("Requesting OAuth2 access token from {}", tokenUri);

        HttpRequest request = buildTokenRequest();

        HttpResponse<String> response;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofString());
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to request OAuth2 access token from " + tokenUri, e);
        }

        if (!HttpUtil.isSuccessStatusCode(response.statusCode())) {
            throw new DebeziumException("OAuth2 token request failed with status " + response.statusCode()
                    + ": " + response.body());
        }

        OAuth2TokenResponse tokenResponse;
        try {
            tokenResponse = mapper.readValue(response.body(), OAuth2TokenResponse.class);
        }
        catch (IOException e) {
            throw new DebeziumException("Could not parse OAuth2 token response", e);
        }

        String token = tokenResponse.getAccessToken();
        if (token == null || token.isEmpty()) {
            throw new DebeziumException("OAuth2 token response did not contain an access_token");
        }

        accessToken = token;
        long expiresIn = tokenResponse.getExpiresIn();
        long bufferedExpiresInMs = (long) (EXPIRATION_BUFFER_MULTIPLIER * expiresIn * 1000);
        tokenExpiresAt = Instant.now(clock).plusMillis(bufferedExpiresInMs);

        LOGGER.info("OAuth2 token acquired (expires in {}s)", expiresIn);
    }

    @Override
    public synchronized void setAuthorizationHeader(HttpRequest.Builder httpRequestBuilder, String bodyContent, UUID messageId) {
        if (accessToken == null) {
            throw new DebeziumException("Must authenticate before setting authorization header");
        }
        httpRequestBuilder.setHeader("Authorization", "Bearer " + accessToken);
    }

    private HttpRequest buildTokenRequest() {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(tokenUri)
                .timeout(httpTimeout);

        if (tokenHttpMethod == TokenHttpMethod.GET) {
            // Non-standard: credentials via Basic auth header, no body
            String credentials = Base64.getEncoder().encodeToString(
                    (clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));
            requestBuilder.GET()
                    .setHeader("Authorization", "Basic " + credentials);
        }
        else {
            // Standard POST with form-urlencoded body
            StringBuilder body = new StringBuilder("grant_type=client_credentials");

            if (scope != null && !scope.isEmpty()) {
                body.append("&scope=").append(urlEncode(scope));
            }

            if (clientAuthMethod == ClientAuthMethod.CLIENT_SECRET_POST) {
                body.append("&client_id=").append(urlEncode(clientId));
                body.append("&client_secret=").append(urlEncode(clientSecret));
            }

            for (Map.Entry<String, String> entry : additionalParams.entrySet()) {
                body.append("&").append(urlEncode(entry.getKey()))
                        .append("=").append(urlEncode(entry.getValue()));
            }

            requestBuilder.POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                    .setHeader("Content-Type", "application/x-www-form-urlencoded");

            if (clientAuthMethod == ClientAuthMethod.CLIENT_SECRET_BASIC) {
                String credentials = Base64.getEncoder().encodeToString(
                        (clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));
                requestBuilder.setHeader("Authorization", "Basic " + credentials);
            }
        }

        return requestBuilder.build();
    }

    private static String urlEncode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
