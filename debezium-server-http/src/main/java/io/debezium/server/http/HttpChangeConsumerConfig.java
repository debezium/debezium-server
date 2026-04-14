/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http;

import java.time.Duration;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link HttpChangeConsumer}.
 */
public class HttpChangeConsumerConfig {

    public static final Field URL = Field.create("url")
            .withDisplayName("Webhook URL")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The HTTP webhook destination URL to send change events to.");

    public static final Field TIMEOUT_MS = Field.create("timeout.ms")
            .withDisplayName("Client timeout (ms)")
            .withType(ConfigDef.Type.LONG)
            .withDefault(60000L)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("HTTP client timeout in milliseconds.");

    public static final Field RETRIES = Field.create("retries")
            .withDisplayName("Max retries")
            .withType(ConfigDef.Type.INT)
            .withDefault(5)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum number of retry attempts for failed HTTP requests.");

    public static final Field RETRY_INTERVAL_MS = Field.create("retry.interval.ms")
            .withDisplayName("Retry interval (ms)")
            .withType(ConfigDef.Type.LONG)
            .withDefault(1000L)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Interval in milliseconds between retry attempts.");

    public static final Field HEADERS_ENCODE_BASE64 = Field.create("headers.encode.base64")
            .withDisplayName("Encode headers in Base64")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(true)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Whether to encode custom header values in Base64.");

    public static final Field HEADERS_PREFIX = Field.create("headers.prefix")
            .withDisplayName("Headers prefix")
            .withType(ConfigDef.Type.STRING)
            .withDefault("X-DEBEZIUM-")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Prefix to prepend to custom header names.");

    // Batch configuration
    public static final Field BATCH_ENABLED = Field.create("batch.enabled")
            .withDisplayName("Batch mode enabled")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Whether to send events in batches.");

    public static final Field BATCH_MAX_SIZE = Field.create("batch.max-size")
            .withDisplayName("Batch maximum size")
            .withType(ConfigDef.Type.INT)
            .withDefault(200)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum number of events to include in a single batch.");

    // Authentication configuration
    public static final String JWT_AUTHENTICATION = "jwt";
    public static final String STANDARD_WEBHOOKS_AUTHENTICATION = "standard-webhooks";
    public static final String OAUTH2_AUTHENTICATION = "oauth2";

    public static final Field AUTHENTICATION_TYPE = Field.create("authentication.type")
            .withDisplayName("Authentication type")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Type of authentication to use. Valid values: 'jwt', 'oauth2', 'standard-webhooks'.");

    // JWT Authentication fields
    public static final Field JWT_USERNAME = Field.create("authentication.jwt.username")
            .withDisplayName("JWT username")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Username for JWT authentication.");

    public static final Field JWT_PASSWORD = Field.create("authentication.jwt.password")
            .withDisplayName("JWT password")
            .withType(ConfigDef.Type.PASSWORD)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Password for JWT authentication.");

    public static final Field JWT_URL = Field.create("authentication.jwt.url")
            .withDisplayName("JWT authentication URL")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Base URL for JWT authentication service.");

    public static final Field JWT_TOKEN_EXPIRATION = Field.create("authentication.jwt.token_expiration")
            .withDisplayName("JWT token expiration (minutes)")
            .withType(ConfigDef.Type.LONG)
            .withDefault(60L)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("JWT token expiration time in minutes.");

    public static final Field JWT_REFRESH_TOKEN_EXPIRATION = Field.create("authentication.jwt.refresh_token_expiration")
            .withDisplayName("JWT refresh token expiration (minutes)")
            .withType(ConfigDef.Type.LONG)
            .withDefault(1440L)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("JWT refresh token expiration time in minutes.");

    // OAuth2 Authentication fields
    public static final Field OAUTH2_CLIENT_ID = Field.create("authentication.oauth2.client-id")
            .withDisplayName("OAuth2 client ID")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Client ID for OAuth2 authentication.");

    public static final Field OAUTH2_CLIENT_SECRET = Field.create("authentication.oauth2.client-secret")
            .withDisplayName("OAuth2 client secret")
            .withType(ConfigDef.Type.PASSWORD)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Client secret for OAuth2 authentication.");

    public static final Field OAUTH2_TOKEN_URL = Field.create("authentication.oauth2.token-url")
            .withDisplayName("OAuth2 token URL")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Token endpoint URL for OAuth2 authentication.");

    public static final Field OAUTH2_SCOPE = Field.create("authentication.oauth2.scope")
            .withDisplayName("OAuth2 scope")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Scope for OAuth2 authentication.");

    // Standard Webhooks Authentication fields
    public static final Field WEBHOOK_SECRET = Field.create("authentication.webhook.secret")
            .withDisplayName("Webhook secret")
            .withType(ConfigDef.Type.PASSWORD)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Secret key for Standard Webhooks authentication.");

    // Instance fields
    private String url;
    private Duration timeoutDuration;
    private int retries;
    private Duration retryInterval;
    private boolean headersEncodeBase64;
    private String headersPrefix;
    private boolean batchEnabled;
    private int batchMaxSize;

    // Authentication fields
    private String authenticationType;
    private String jwtUsername;
    private String jwtPassword;
    private String jwtUrl;
    private Long jwtTokenExpiration;
    private Long jwtRefreshTokenExpiration;
    private String oauth2ClientId;
    private String oauth2ClientSecret;
    private String oauth2TokenUrl;
    private String oauth2Scope;
    private String webhookSecret;

    public HttpChangeConsumerConfig(Configuration config) {
        init(config);
    }

    protected void init(Configuration config) {
        url = config.getString(URL);
        timeoutDuration = Duration.ofMillis(config.getLong(TIMEOUT_MS));
        retries = config.getInteger(RETRIES);
        retryInterval = Duration.ofMillis(config.getLong(RETRY_INTERVAL_MS));
        headersEncodeBase64 = config.getBoolean(HEADERS_ENCODE_BASE64);
        headersPrefix = config.getString(HEADERS_PREFIX);
        batchEnabled = config.getBoolean(BATCH_ENABLED);
        batchMaxSize = config.getInteger(BATCH_MAX_SIZE);

        // Authentication configuration
        authenticationType = config.getString(AUTHENTICATION_TYPE);
        jwtUsername = config.getString(JWT_USERNAME);
        jwtPassword = config.getString(JWT_PASSWORD);
        jwtUrl = config.getString(JWT_URL);
        jwtTokenExpiration = config.getLong(JWT_TOKEN_EXPIRATION);
        jwtRefreshTokenExpiration = config.getLong(JWT_REFRESH_TOKEN_EXPIRATION);
        oauth2ClientId = config.getString(OAUTH2_CLIENT_ID);
        oauth2ClientSecret = config.getString(OAUTH2_CLIENT_SECRET);
        oauth2TokenUrl = config.getString(OAUTH2_TOKEN_URL);
        oauth2Scope = config.getString(OAUTH2_SCOPE);
        webhookSecret = config.getString(WEBHOOK_SECRET);
    }

    public String getUrl() {
        return url;
    }

    public Duration getTimeoutDuration() {
        return timeoutDuration;
    }

    public int getRetries() {
        return retries;
    }

    public Duration getRetryInterval() {
        return retryInterval;
    }

    public boolean isHeadersEncodeBase64() {
        return headersEncodeBase64;
    }

    public String getHeadersPrefix() {
        return headersPrefix;
    }

    public String getAuthenticationType() {
        return authenticationType;
    }

    public String getJwtUsername() {
        return jwtUsername;
    }

    public String getJwtPassword() {
        return jwtPassword;
    }

    public String getJwtUrl() {
        return jwtUrl;
    }

    public Long getJwtTokenExpiration() {
        return jwtTokenExpiration;
    }

    public Long getJwtRefreshTokenExpiration() {
        return jwtRefreshTokenExpiration;
    }

    public String getWebhookSecret() {
        return webhookSecret;
    }

    public boolean isBatchEnabled() {
        return batchEnabled;
    }

    public int getBatchMaxSize() {
        return batchMaxSize;
    }

    public String getOauth2ClientId() {
        return oauth2ClientId;
    }

    public String getOauth2ClientSecret() {
        return oauth2ClientSecret;
    }

    public String getOauth2TokenUrl() {
        return oauth2TokenUrl;
    }

    public String getOauth2Scope() {
        return oauth2Scope;
    }
}
