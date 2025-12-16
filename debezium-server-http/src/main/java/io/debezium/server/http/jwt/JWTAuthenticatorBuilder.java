/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http.jwt;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.util.NoSuchElementException;

import org.eclipse.microprofile.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;

public class JWTAuthenticatorBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(JWTAuthenticatorBuilder.class);
    private static final long HTTP_TIMEOUT = Integer.toUnsignedLong(60000); // Default to 60s

    private static final String PROP_USERNAME = "jwt.username";
    private static final String PROP_PASSWORD = "jwt.password";
    private static final String PROP_URL = "jwt.url";
    private static final String PROP_TOKEN_EXPIRATION = "jwt.token_expiration";
    private static final String PROP_RENEW_TOKEN_EXPIRATION = "jwt.refresh_token_expiration";

    private static final String AUTHENTICATE_PATH = "auth/authenticate";
    private static final String REFRESH_PATH = "auth/refreshToken";

    private URI authUri;
    private URI refreshUri;
    private String username;
    private String password;
    private long tokenExpirationDuration = Integer.toUnsignedLong(60); // Default to 60 min
    private long refreshTokenExpirationDuration = Integer.toUnsignedLong(60 * 24); // Default to 24 hours
    private Duration httpTimeoutDuration = Duration.ofMillis(HTTP_TIMEOUT); // in ms
    private HttpClient client;
    private Clock clock = Clock.systemUTC();

    public static JWTAuthenticatorBuilder fromConfig(Config config, String prop_prefix) {
        JWTAuthenticatorBuilder builder = new JWTAuthenticatorBuilder();

        builder.setUsername(config.getValue(prop_prefix + PROP_USERNAME, String.class));
        builder.setPassword(config.getValue(prop_prefix + PROP_PASSWORD, String.class));

        String uriString = config.getValue(prop_prefix + PROP_URL, String.class);

        try {
            LOGGER.info("Authentication URL is " + uriString + AUTHENTICATE_PATH);
            URI authUri = new URI(uriString + AUTHENTICATE_PATH);
            builder.setAuthUri(authUri);
        }
        catch (URISyntaxException e) {
            throw new DebeziumException("Could not parse authentication URL: " + uriString + AUTHENTICATE_PATH, e);
        }

        try {
            LOGGER.info("Authentication URL is " + uriString + REFRESH_PATH);
            URI refreshUri = new URI(uriString + REFRESH_PATH);
            builder.setRefreshUri(refreshUri);
        }
        catch (URISyntaxException e) {
            throw new DebeziumException("Could not parse refresh URL: " + uriString + REFRESH_PATH, e);
        }

        config.getOptionalValue(prop_prefix + PROP_TOKEN_EXPIRATION, Long.class)
                .ifPresent(builder::setTokenExpirationDuration);

        config.getOptionalValue(prop_prefix + PROP_RENEW_TOKEN_EXPIRATION, Long.class)
                .ifPresent(builder::setRefreshTokenExpirationDuration);

        return builder;
    }

    public void setRefreshUri(URI refreshUri) {
        this.refreshUri = refreshUri;
    }

    public JWTAuthenticatorBuilder setAuthUri(URI authUri) {
        this.authUri = authUri;
        return this;
    }

    public JWTAuthenticatorBuilder setUsername(String username) {
        this.username = username;
        return this;
    }

    public JWTAuthenticatorBuilder setPassword(String password) {
        this.password = password;
        return this;
    }

    public JWTAuthenticatorBuilder setTokenExpirationDuration(long tokenExpirationDuration) {
        this.tokenExpirationDuration = tokenExpirationDuration;
        return this;
    }

    public JWTAuthenticatorBuilder setRefreshTokenExpirationDuration(long refreshTokenExpirationDuration) {
        this.refreshTokenExpirationDuration = refreshTokenExpirationDuration;
        return this;
    }

    public JWTAuthenticatorBuilder setHttpTimeoutDuration(long timeoutDuration) {
        this.httpTimeoutDuration = Duration.ofMillis(timeoutDuration);
        return this;
    }

    public JWTAuthenticatorBuilder setHttpClient(HttpClient client) {
        this.client = client;
        return this;
    }

    public JWTAuthenticator build() {
        if (authUri == null) {
            String msg = "Cannot build JWTAuthenticator.  Initialization authorization URI must be set.";
            LOGGER.error(msg);
            throw new NoSuchElementException(msg);
        }

        if (refreshUri == null) {
            String msg = "Cannot build JWTAuthenticator.  Refresh authorization URI must be set.";
            LOGGER.error(msg);
            throw new NoSuchElementException(msg);
        }

        if (username == null) {
            String msg = "Cannot build JWTAuthenticator.  Username must be set.";
            LOGGER.error(msg);
            throw new NoSuchElementException(msg);
        }

        if (password == null) {
            String msg = "Cannot build JWTAuthenticator.  Password must be set.";
            LOGGER.error(msg);
            throw new NoSuchElementException(msg);
        }

        if (client == null) {
            client = HttpClient.newHttpClient();
        }

        return new JWTAuthenticator(client, clock, authUri, refreshUri, username, password, tokenExpirationDuration, refreshTokenExpirationDuration, httpTimeoutDuration);
    }
}
