/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http.jwt;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;

import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;

public class JWTAuthenticatorBuilderTest {

    @Test
    public void verifyBuild() throws URISyntaxException {
        JWTAuthenticatorBuilder builder = new JWTAuthenticatorBuilder();
        builder.setAuthUri(new URI("http://test.com/auth/authenticate"));
        builder.setRefreshUri(new URI("http://test.com/auth/refreshToken"));
        builder.setUsername("testUser");
        builder.setPassword("testPassword");

        JWTAuthenticator authenticator = builder.build();
    }

    @Test
    public void verifyBuildFromConfig() throws URISyntaxException {
        Map<String, Object> configValues = Map.of("debezium.sink.http.jwt.url", "http://test.com/",
                "debezium.sink.http.jwt.username", "testUser",
                "debezium.sink.http.jwt.password", "testPassword");

        Config result = mock(Config.class);

        for (Map.Entry<String, Object> entry : configValues.entrySet()) {
            Object value = entry.getValue();
            when(result.getValue(eq(entry.getKey()), any())).thenReturn(value);
            when(result.getOptionalValue(eq(entry.getKey()), any())).thenReturn(Optional.of(value));
        }

        JWTAuthenticatorBuilder builder = JWTAuthenticatorBuilder.fromConfig(result, "debezium.sink.http.");
        builder.setRefreshUri(new URI("http://test.com"));
        builder.setUsername("testUser");
        builder.setPassword("testPassword");

        JWTAuthenticator authenticator = builder.build();
    }
}
