/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http.jwt;

import static java.net.HttpURLConnection.HTTP_OK;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.server.http.Authenticator;

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

    private final String username;
    private final String password;
    private final long tokenExpirationDuration; // minutes
    private final long refreshTokenExpirationDuration; // minutes

    private String jwtToken;
    private String jwtRefreshToken;
    private final HttpClient client;
    private final HttpRequest.Builder authRequestBuilder;
    private final HttpRequest.Builder refreshRequestBuilder;
    private final ObjectMapper mapper;

    private AuthenticationState authenticationState;
    private DateTime expirationDateTime;

    JWTAuthenticator(URI authUri, URI refreshUri, String username, String password, long tokenExpirationDuration, long refreshTokenExpirationDuration,
                     Duration httpTimeoutDuration) {
        this.username = username;
        this.password = password;
        this.tokenExpirationDuration = tokenExpirationDuration;
        this.refreshTokenExpirationDuration = refreshTokenExpirationDuration;

        mapper = new ObjectMapper();
        client = HttpClient.newHttpClient();
        authRequestBuilder = HttpRequest.newBuilder(authUri).timeout(httpTimeoutDuration);
        authRequestBuilder.setHeader("content-type", "application/json");
        refreshRequestBuilder = HttpRequest.newBuilder(refreshUri).timeout(httpTimeoutDuration);
        refreshRequestBuilder.setHeader("content-type", "application/json");

        authenticationState = AuthenticationState.NOT_AUTHENTICATED;
        // initialize to value before now to correspond to not authenticated state
        expirationDateTime = DateTime.now().minusDays(1);
    }

    @VisibleForTesting
    HttpRequest generateInitialAuthenticationRequest() throws IOException {
        JWTAuthorizationInitialRequest payload = new JWTAuthorizationInitialRequest(username, password, tokenExpirationDuration, refreshTokenExpirationDuration);

        StringWriter payloadWriter = new StringWriter();
        try {
            mapper.writeValue(payloadWriter, payload);
        }
        catch (IOException e) {
            LOGGER.error("Could not serialize JWTAuthorizationRequest object to JSON.");
            throw e;
        }

        String payloadJSON = payloadWriter.toString();
        HttpRequest.Builder builder = authRequestBuilder.POST(HttpRequest.BodyPublishers.ofString(payloadJSON));

        return builder.build();
    }

    private void checkAuthenticationExpired() {
        if (authenticationState == AuthenticationState.ACTIVE) {
            if (expirationDateTime.isBeforeNow()) {
                authenticationState = AuthenticationState.EXPIRED;
            }
        }
    }

    @VisibleForTesting
    HttpRequest generateRefreshAuthenticationRequest() throws IOException {
        checkAuthenticationExpired();

        if (authenticationState == AuthenticationState.NOT_AUTHENTICATED || authenticationState == AuthenticationState.FAILED_AUTHENTICATION) {
            String msg = "Must perform initial authentication successfully before attempting to refresh authentication";
            LOGGER.error(msg);
            throw new IllegalStateException(msg);
        }

        JWTAuthorizationRefreshRequest payload = new JWTAuthorizationRefreshRequest(jwtRefreshToken, tokenExpirationDuration, refreshTokenExpirationDuration);

        StringWriter payloadWriter = new StringWriter();
        try {
            mapper.writeValue(payloadWriter, payload);
        }
        catch (IOException e) {
            LOGGER.error("Could not serialize JWTAuthorizationRequest object to JSON.");
            throw e;
        }

        String payloadJSON = payloadWriter.toString();
        HttpRequest.Builder builder = authRequestBuilder.POST(HttpRequest.BodyPublishers.ofString(payloadJSON));

        return builder.build();
    }

    public void addAuthorizationHeader(HttpRequest.Builder httpRequestBuilder) {
        checkAuthenticationExpired();
        if (authenticationState == AuthenticationState.NOT_AUTHENTICATED || authenticationState == AuthenticationState.FAILED_AUTHENTICATION) {
            String msg = "Must successfully authenticate against JWT endpoint before you can add the authorization information to the HTTP header.";
            LOGGER.error(msg);
            throw new IllegalStateException(msg);
        }
        else if (authenticationState == AuthenticationState.EXPIRED) {
            String msg = "JWT authentication is expired. Must renew authentication before you can add the authorization information to the HTTP header.";
            LOGGER.error(msg);
            throw new IllegalStateException(msg);
        }

        httpRequestBuilder.header("Authorization", "Bearer: " + jwtToken);
    }

    public boolean authenticate() throws InterruptedException, IOException {
        checkAuthenticationExpired();

        HttpResponse<String> r;
        HttpRequest request;
        JWTAuthorizationResponse response;

        if (authenticationState == AuthenticationState.ACTIVE) {
            return true;
        }
        else if (authenticationState == AuthenticationState.NOT_AUTHENTICATED || authenticationState == AuthenticationState.FAILED_AUTHENTICATION) {
            request = generateInitialAuthenticationRequest();
        }
        else if (authenticationState == AuthenticationState.EXPIRED) {
            request = generateRefreshAuthenticationRequest();
        }
        else {
            // we should never get here...
            String msg = "Reached invalid authentication state.";
            LOGGER.error(msg);
            throw new IllegalStateException(msg);
        }

        try {
            r = client.send(request, HttpResponse.BodyHandlers.ofString());
        }
        catch (IOException ioe) {
            throw new InterruptedException(ioe.toString());
        }

        if (r.statusCode() == HTTP_OK) {
            String responseBody = r.body();

            try {
                response = mapper.readValue(responseBody, JWTAuthorizationResponse.class);
            }
            catch (IOException e) {
                LOGGER.error("Could not deserialize JWT authorization response.");
                throw e;
            }

            jwtToken = response.getJwt();
            jwtRefreshToken = response.getJwtRefreshToken();

            // in ms
            long expirationDuration = (long) (EXPIRATION_DURATION_MULTIPLIER * response.getExpiresIn());
            expirationDateTime = DateTime.now().plus(expirationDuration);

            authenticationState = AuthenticationState.ACTIVE;

            return true;
        }

        authenticationState = AuthenticationState.FAILED_AUTHENTICATION;
        LOGGER.error("JWT Authentication failure. Check credentials.");

        return false;
    }
}
