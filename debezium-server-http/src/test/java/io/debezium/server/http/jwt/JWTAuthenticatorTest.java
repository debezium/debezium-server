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
import java.util.Optional;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

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

        Assertions.assertEquals(initialRequest.uri(),
                authURI);
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
        authenticator.setAuthenticationState(JWTAuthenticator.AuthenticationState.EXPIRED);

        HttpRequest initialRequest = authenticator.generateRefreshAuthenticationRequest();

        Assertions.assertEquals(initialRequest.uri(),
                refreshURI);
        Assertions.assertTrue(initialRequest.method().equalsIgnoreCase("POST"));
        Assertions.assertTrue(initialRequest.bodyPublisher().isPresent());
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
                Duration.ofMillis(100000));

        authenticator.setJwtToken("fakeToken");
        authenticator.setAuthenticationState(JWTAuthenticator.AuthenticationState.ACTIVE);

        URI testURI = new URI("http://test.com/cookies");
        HttpRequest.Builder builder = HttpRequest.newBuilder(testURI);
        authenticator.addAuthorizationHeader(builder);
        HttpRequest request = builder.build();

        HttpHeaders headers = request.headers();
        Optional<String> authValue = headers.firstValue("Authorization");

        HttpRequest initialRequest = authenticator.generateInitialAuthenticationRequest();

        Assertions.assertTrue(authValue.isPresent());
        Assertions.assertTrue(authValue.get().startsWith("Bearer: "));
    }
}
