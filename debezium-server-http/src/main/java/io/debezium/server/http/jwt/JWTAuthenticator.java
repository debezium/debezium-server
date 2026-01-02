/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http.jwt;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.server.http.Authenticator;
import io.debezium.server.http.HttpUtil;

/**
 * Implements the logic for authenticating against an endpoint supporting the
 * JSON Web Tokens (JWT) scheme.  Once authentication is successful, the
 * authenticator can add the authentication details to the header of an HTTP
 * request using a <a href="https://docs.oracle.com/en/java/javase/11/docs/api/java.net.http/java/net/http/HttpRequest.html">HTTPRequest.Builder</a> instance. After the initial authentication
 * is successful, additional authentication attempts will refresh the token.
 */
public class JWTAuthenticator implements Authenticator {
    private enum AuthenticationState {
        NOT_AUTHENTICATED, // before first successful authentication
        FAILED_AUTHENTICATION, // attempted authentication but it failed
        ACTIVE, // successful authentication and token is still valid
        EXPIRED // successful authentication but token has expired
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(JWTAuthenticator.class);

    // Want to authenticate before expiration
    private static final double EXPIRATION_DURATION_MULTIPLIER = 0.9;

    private final Clock clock;
    private final String username;
    private final String password;
    private final long tokenExpirationDuration; // minutes
    private final long refreshTokenExpirationDuration; // minutes

    private String jwtToken;
    private String jwtRefreshToken;
    private final HttpClient client;
    private final HttpRequest.Builder authRequestBuilder;
    private final HttpRequest.Builder refreshRequestBuilder;
    private final ObjectMapper mapper = new ObjectMapper();

    private AuthenticationState authenticationState;
    private Instant expirationDateTime;

    JWTAuthenticator(
                     HttpClient client,
                     Clock clock,
                     URI authUri,
                     URI refreshUri,
                     String username,
                     String password,
                     long tokenExpirationDuration,
                     long refreshTokenExpirationDuration,
                     Duration httpTimeoutDuration) {
        this.client = client;
        this.clock = clock;
        this.username = username;
        this.password = password;
        this.tokenExpirationDuration = tokenExpirationDuration;
        this.refreshTokenExpirationDuration = refreshTokenExpirationDuration;
        authRequestBuilder = HttpRequest.newBuilder(authUri)
                .timeout(httpTimeoutDuration)
                .setHeader("content-type", "application/json");
        refreshRequestBuilder = HttpRequest.newBuilder(refreshUri)
                .timeout(httpTimeoutDuration)
                .setHeader("content-type", "application/json");

        authenticationState = AuthenticationState.NOT_AUTHENTICATED;
        // initialize to value before now to correspond to not authenticated state
        expirationDateTime = Instant.now(clock).minus(1, ChronoUnit.DAYS);
    }

    private HttpRequest generateInitialAuthenticationRequest() {
        JWTAuthorizationInitialRequest payload = new JWTAuthorizationInitialRequest(username, password, tokenExpirationDuration, refreshTokenExpirationDuration);

        String payloadJson;
        try {
            payloadJson = mapper.writeValueAsString(payload);
        }
        catch (JsonProcessingException e) {
            throw new DebeziumException("Could not serialize JWTAuthorizationRequest object to JSON.", e);
        }

        return authRequestBuilder.POST(HttpRequest.BodyPublishers.ofString(payloadJson))
                .build();
    }

    private HttpRequest generateRefreshAuthenticationRequest() {
        if (authenticationState == AuthenticationState.NOT_AUTHENTICATED || authenticationState == AuthenticationState.FAILED_AUTHENTICATION) {
            throw new DebeziumException("Must perform initial authentication successfully before attempting to refresh authentication");
        }

        JWTAuthorizationRefreshRequest payload = new JWTAuthorizationRefreshRequest(jwtRefreshToken, tokenExpirationDuration, refreshTokenExpirationDuration);

        String payloadJson;
        try {
            payloadJson = mapper.writeValueAsString(payload);
        }
        catch (JsonProcessingException e) {
            throw new DebeziumException("Could not serialize JWTAuthorizationRequest object to JSON.", e);
        }

        return refreshRequestBuilder.POST(HttpRequest.BodyPublishers.ofString(payloadJson))
                .build();
    }

    public void setAuthorizationHeader(HttpRequest.Builder httpRequestBuilder, final String bodyContent, final UUID messageId) {
        checkAuthenticationExpired();
        if (authenticationState == AuthenticationState.NOT_AUTHENTICATED || authenticationState == AuthenticationState.FAILED_AUTHENTICATION) {
            throw new DebeziumException("Must successfully authenticate against JWT endpoint before you can add the authorization information to the HTTP header.");
        }
        else if (authenticationState == AuthenticationState.EXPIRED) {
            throw new DebeziumException("JWT authentication is expired. Must renew authentication before you can add the authorization information to the HTTP header.");
        }

        httpRequestBuilder.setHeader("Authorization", "Bearer: " + jwtToken);
    }

    public void authenticate() throws InterruptedException {
        checkAuthenticationExpired();

        HttpRequest request = switch (authenticationState) {
            case ACTIVE -> null;
            case NOT_AUTHENTICATED, FAILED_AUTHENTICATION -> generateInitialAuthenticationRequest();
            case EXPIRED -> generateRefreshAuthenticationRequest();
        };

        if (request == null) {
            return;
        }

        HttpResponse<String> r;
        try {
            r = client.send(request, HttpResponse.BodyHandlers.ofString());
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to send authentication request", e);
        }

        if (HttpUtil.isSuccessStatusCode(r.statusCode())) {
            String responseBody = r.body();

            JWTAuthorizationResponse response;
            try {
                response = mapper.readValue(responseBody, JWTAuthorizationResponse.class);
            }
            catch (IOException e) {
                throw new DebeziumException("Could not deserialize JWT authorization response.", e);
            }

            jwtToken = response.getJwt();
            jwtRefreshToken = response.getJwtRefreshToken();

            long expirationInMs = (long) (EXPIRATION_DURATION_MULTIPLIER * response.getExpiresIn());
            expirationDateTime = Instant.now(clock).plus(expirationInMs, ChronoUnit.MILLIS);

            authenticationState = AuthenticationState.ACTIVE;
        }
        else {
            authenticationState = AuthenticationState.FAILED_AUTHENTICATION;
            LOGGER.error("JWT Authentication failure. Check credentials.");
            throw new DebeziumException("Failed to authenticate successfully.  Cannot continue.");
        }
    }

    private void checkAuthenticationExpired() {
        if (authenticationState == AuthenticationState.ACTIVE) {
            if (expirationDateTime.isBefore(Instant.now(clock))) {
                authenticationState = AuthenticationState.EXPIRED;
            }
        }
    }
}
