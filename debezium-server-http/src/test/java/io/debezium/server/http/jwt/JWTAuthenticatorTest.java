/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http.jwt;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JWTAuthenticatorTest {

    @Test
    public void generateInitialAuthenticationRequest() throws URISyntaxException {
        URI authURI = new URI("http://test.com/auth/authenticate");
        URI refreshURI = new URI("http://test.com/auth/refreshToken");
        JWTAuthenticator authenticator = new JWTAuthenticator(authURI,
                refreshURI,
                "testUser",
                "testPassword",
                10000,
                10000,
                Duration.ofMillis(100000));

        HttpRequest initialRequest = authenticator.generateInitialAuthenticationRequest();

        Assertions.assertEquals(authURI, initialRequest.uri());
        Assertions.assertTrue(initialRequest.method().equalsIgnoreCase("POST"));
        Assertions.assertTrue(initialRequest.bodyPublisher().isPresent());
    }

    @Test
    public void generateRefreshAuthenticationRequest() throws URISyntaxException {
        URI authURI = new URI("http://test.com/auth/authenticate");
        URI refreshURI = new URI("http://test.com/auth/refreshToken");
        JWTAuthenticator authenticator = new JWTAuthenticator(authURI,
                refreshURI,
                "testUser",
                "testPassword",
                10000,
                10000,
                Duration.ofMillis(100000));

        authenticator.setJwtToken("fakeToken");
        authenticator.setJwtRefreshToken("fakeRefreshToken");
        authenticator.setAuthenticationState(JWTAuthenticator.AuthenticationState.EXPIRED, Optional.empty());

        HttpRequest refreshRequest = authenticator.generateRefreshAuthenticationRequest();

        Assertions.assertEquals(refreshURI, refreshRequest.uri());
        Assertions.assertTrue(refreshRequest.method().equalsIgnoreCase("POST"));
        Assertions.assertTrue(refreshRequest.bodyPublisher().isPresent());
    }

    @Test
    public void addAuthorizationHeader() throws URISyntaxException {
        URI authURI = new URI("http://test.com/auth/authenticate");
        URI refreshURI = new URI("http://test.com/auth/refreshToken");
        JWTAuthenticator authenticator = new JWTAuthenticator(authURI,
                refreshURI,
                "testUser",
                "testPassword",
                10000,
                10000,
                Duration.ofDays(100000));

        authenticator.setJwtToken("fakeToken");
        authenticator.setAuthenticationState(JWTAuthenticator.AuthenticationState.ACTIVE,
                Optional.of(Instant.now().plusSeconds(10000)));

        URI testURI = new URI("http://test.com/cookies");
        HttpRequest.Builder builder = HttpRequest.newBuilder(testURI);
        authenticator.setAuthorizationHeader(builder, "", new UUID(0, 0));
        HttpRequest request = builder.build();

        HttpHeaders headers = request.headers();
        Optional<String> authValue = headers.firstValue("Authorization");

        HttpRequest initialRequest = authenticator.generateInitialAuthenticationRequest();

        Assertions.assertTrue(authValue.isPresent());
        Assertions.assertEquals("Bearer: fakeToken", authValue.get());
    }
}
