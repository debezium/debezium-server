/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.config.Field;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.DebeziumServerSink;
import io.debezium.server.http.jwt.JWTAuthenticatorBuilder;
import io.debezium.server.http.oauth2.OAuth2AuthenticatorBuilder;
import io.debezium.server.http.webhooks.StandardWebhooksAuthenticatorBuilder;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Implementation of the consumer that delivers the messages to an HTTP Webhook destination.
 *
 * @author Chris Baumbauer
 */
@Named("http")
@Dependent
public class HttpChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>, DebeziumServerSink {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpChangeConsumer.class);

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    private static final String PROP_PREFIX = "debezium.sink.http.";

    private HttpChangeConsumerConfig config;
    private HttpClient client;
    private HttpRequest.Builder baseRequestBuilder;

    // not null if using authentication; null otherwise
    private Authenticator authenticator;

    // If this is running as a Knative object, then expect the sink URL to be located in `K_SINK`
    // as per https://knative.dev/development/eventing/custom-event-source/sinkbinding/
    @PostConstruct
    void connect() throws URISyntaxException {
        final Config config = ConfigProvider.getConfig();
        initWithConfig(config);
    }

    @PreDestroy
    @Override
    public void close() {
        LOGGER.info("Closing HTTP sink");

        client.close();
    }

    @VisibleForTesting
    void initWithConfig(Config mpConfig) throws URISyntaxException {
        String sinkUrl;
        String contentType;

        // Convert MicroProfile Config to Debezium Configuration
        io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));
        this.config = new HttpChangeConsumerConfig(configuration);

        client = createHttpClient();
        String sink = System.getenv("K_SINK");

        if (sink != null) {
            sinkUrl = sink;
        }
        else {
            sinkUrl = this.config.getUrl();
        }

        contentType = switch (mpConfig.getValue("debezium.format.value", String.class).toLowerCase()) {
            case "avro" -> "avro/bytes";
            case "cloudevents" -> "application/cloudevents+json";
            case "json" -> "application/json";
            // Note: will default to JSON if it cannot be determined, but should not reach this point
            default -> "application/json";
        };

        authenticator = buildAuthenticator(configuration);

        LOGGER.info("Using http content-type type {}", contentType);
        LOGGER.info("Using sink URL: {}", sinkUrl);
        LOGGER.info("Batch mode: {}", config.isBatchEnabled() ? "enabled (max-size=" + config.getBatchMaxSize() + ")" : "disabled");
        baseRequestBuilder = HttpRequest
                .newBuilder(new URI(sinkUrl))
                .timeout(this.config.getTimeoutDuration())
                .setHeader("content-type", contentType);
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        if (config.isBatchEnabled()) {
            handleBatchAggregated(records, committer);
        }
        else {
            handleBatchIndividual(records, committer);
        }
    }

    private void handleBatchIndividual(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);

            UUID messageId = UUID.randomUUID();
            LOGGER.trace("Using message ID '{}'", messageId);

            if (record.value() != null) {
                int attempts = 0;
                while (!recordSent(record, messageId)) {
                    attempts++;
                    if (attempts >= config.getRetries()) {
                        throw new DebeziumException("Exceeded maximum number of attempts to publish event " + record);
                    }
                    Metronome.sleeper(config.getRetryInterval(), Clock.SYSTEM).pause();
                }
                committer.markProcessed(record);
            }
        }

        committer.markBatchFinished();
    }

    private void handleBatchAggregated(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        // Collect records with non-null values, preserving the record references for markProcessed
        List<ChangeEvent<Object, Object>> nonNullRecords = new ArrayList<>();
        for (ChangeEvent<Object, Object> record : records) {
            if (record.value() != null) {
                nonNullRecords.add(record);
            }
        }

        if (nonNullRecords.isEmpty()) {
            committer.markBatchFinished();
            return;
        }

        // Chunk records into sub-batches of at most batchMaxSize
        for (int from = 0; from < nonNullRecords.size(); from += config.getBatchMaxSize()) {
            int to = Math.min(from + config.getBatchMaxSize(), nonNullRecords.size());
            List<ChangeEvent<Object, Object>> chunk = nonNullRecords.subList(from, to);

            List<String> values = new ArrayList<>(chunk.size());
            for (ChangeEvent<Object, Object> record : chunk) {
                values.add((String) record.value());
            }

            // Values are assumed to be pre-serialized JSON from the Debezium format serializer
            String batchPayload = "[" + String.join(",", values) + "]";

            Map<String, String> chunkHeaders = convertHeaders(chunk.get(0));
            UUID messageId = UUID.randomUUID();
            int attempts = 0;
            while (!batchSent(batchPayload, messageId, chunkHeaders)) {
                attempts++;
                if (attempts >= config.getRetries()) {
                    throw new DebeziumException("Exceeded maximum number of attempts to publish batch of " + chunk.size() + " events");
                }
                Metronome.sleeper(config.getRetryInterval(), Clock.SYSTEM).pause();
            }

            // Mark records processed immediately after their chunk is successfully sent
            for (ChangeEvent<Object, Object> record : chunk) {
                committer.markProcessed(record);
            }
        }

        committer.markBatchFinished();
    }

    private boolean batchSent(String batchPayload, UUID messageId, Map<String, String> headers) throws InterruptedException {
        HttpRequest.Builder requestBuilder = baseRequestBuilder.copy()
                .POST(HttpRequest.BodyPublishers.ofString(batchPayload));

        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String headerValue = entry.getValue();
            if (config.isHeadersEncodeBase64()) {
                headerValue = Base64.getEncoder().encodeToString(headerValue.getBytes(StandardCharsets.UTF_8));
            }
            requestBuilder.header(config.getHeadersPrefix() + entry.getKey().toUpperCase(Locale.ROOT), headerValue);
        }

        if (authenticator != null) {
            authenticator.authenticate();
            authenticator.setAuthorizationHeader(requestBuilder, batchPayload, messageId);
        }

        HttpResponse<String> r;
        try {
            r = client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
        }
        catch (IOException ioe) {
            LOGGER.info("Failed to send batch: {}", ioe.getMessage());
            return false;
        }

        if (HttpUtil.isSuccessStatusCode(r.statusCode())) {
            return true;
        }
        else {
            LOGGER.info("Failed to publish batch: {}", r.body());
            return false;
        }
    }

    private Authenticator buildAuthenticator(io.debezium.config.Configuration configuration) {
        String authenticationType = config.getAuthenticationType();

        if (authenticationType == null) {
            return null;
        }

        return switch (authenticationType.toLowerCase()) {
            case HttpChangeConsumerConfig.JWT_AUTHENTICATION -> buildJwtAuthenticator();
            case HttpChangeConsumerConfig.OAUTH2_AUTHENTICATION -> buildOauth2Authenticator();
            case HttpChangeConsumerConfig.STANDARD_WEBHOOKS_AUTHENTICATION -> buildStandardWebhooksAuthenticator();
            default -> throw new DebeziumException(
                    "Unknown value '" + authenticationType + "' encountered for property " + PROP_PREFIX + "authentication.type");
        };
    }

    private Authenticator buildJwtAuthenticator() {
        try {
            JWTAuthenticatorBuilder builder = new JWTAuthenticatorBuilder()
                    .setUsername(config.getJwtUsername())
                    .setPassword(config.getJwtPassword())
                    .setAuthUri(new java.net.URI(config.getJwtUrl() + "auth/authenticate"))
                    .setHttpClient(client);

            // setRefreshUri returns void, so call it separately
            builder.setRefreshUri(new java.net.URI(config.getJwtUrl() + "auth/refreshToken"));

            // Set token expirations if provided (with defaults)
            if (config.getJwtTokenExpiration() != null) {
                builder.setTokenExpirationDuration(config.getJwtTokenExpiration());
            }
            if (config.getJwtRefreshTokenExpiration() != null) {
                builder.setRefreshTokenExpirationDuration(config.getJwtRefreshTokenExpiration());
            }

            return builder.build();
        }
        catch (java.net.URISyntaxException e) {
            throw new DebeziumException("Could not parse JWT authentication URL: " + config.getJwtUrl(), e);
        }
    }

    private Authenticator buildOauth2Authenticator() {
        try {
            OAuth2AuthenticatorBuilder builder = new OAuth2AuthenticatorBuilder()
                    .setClientId(config.getOauth2ClientId())
                    .setClientSecret(config.getOauth2ClientSecret())
                    .setTokenUri(new java.net.URI(config.getOauth2TokenUrl()))
                    .setHttpClient(client);

            if (config.getOauth2Scope() != null) {
                builder.setScope(config.getOauth2Scope());
            }

            return builder.build();
        }
        catch (java.net.URISyntaxException e) {
            throw new DebeziumException("Could not parse OAuth2 token URL: " + config.getOauth2TokenUrl(), e);
        }
    }

    private Authenticator buildStandardWebhooksAuthenticator() {
        return new StandardWebhooksAuthenticatorBuilder()
                .setSecret(config.getWebhookSecret())
                .build();
    }

    private boolean recordSent(ChangeEvent<Object, Object> record, UUID messageId) throws InterruptedException {
        HttpResponse<String> r;

        HttpRequest.Builder requestBuilder = generateRequest(record);

        if (authenticator != null) {
            authenticator.authenticate();
            authenticator.setAuthorizationHeader(requestBuilder, (String) record.value(), messageId);
        }

        try {
            r = client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
        }
        catch (IOException ioe) {
            LOGGER.info("Failed to send event: {}", ioe.getMessage());
            return false;
        }

        if (HttpUtil.isSuccessStatusCode(r.statusCode())) {
            return true;
        }
        else {
            LOGGER.info("Failed to publish event: {}", r.body());
            return false;
        }
    }

    @VisibleForTesting
    HttpRequest.Builder generateRequest(ChangeEvent<Object, Object> record) {
        String value = (String) record.value();
        HttpRequest.Builder builder = baseRequestBuilder.copy()
                .POST(HttpRequest.BodyPublishers.ofString(value));

        Map<String, String> headers = convertHeaders(record);

        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String headerValue = entry.getValue();
            if (config.isHeadersEncodeBase64()) {
                headerValue = Base64.getEncoder().encodeToString(headerValue.getBytes(StandardCharsets.UTF_8));
            }
            builder.header(config.getHeadersPrefix() + entry.getKey().toUpperCase(Locale.ROOT), headerValue);
        }

        return builder;
    }

    @VisibleForTesting
    HttpClient createHttpClient() {
        return HttpClient.newHttpClient();
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                HttpChangeConsumerConfig.URL,
                HttpChangeConsumerConfig.TIMEOUT_MS,
                HttpChangeConsumerConfig.RETRIES,
                HttpChangeConsumerConfig.RETRY_INTERVAL_MS,
                HttpChangeConsumerConfig.HEADERS_ENCODE_BASE64,
                HttpChangeConsumerConfig.HEADERS_PREFIX,
                HttpChangeConsumerConfig.BATCH_ENABLED,
                HttpChangeConsumerConfig.BATCH_MAX_SIZE,
                HttpChangeConsumerConfig.AUTHENTICATION_TYPE,
                HttpChangeConsumerConfig.JWT_USERNAME,
                HttpChangeConsumerConfig.JWT_PASSWORD,
                HttpChangeConsumerConfig.JWT_URL,
                HttpChangeConsumerConfig.JWT_TOKEN_EXPIRATION,
                HttpChangeConsumerConfig.JWT_REFRESH_TOKEN_EXPIRATION,
                HttpChangeConsumerConfig.OAUTH2_CLIENT_ID,
                HttpChangeConsumerConfig.OAUTH2_CLIENT_SECRET,
                HttpChangeConsumerConfig.OAUTH2_TOKEN_URL,
                HttpChangeConsumerConfig.OAUTH2_SCOPE,
                HttpChangeConsumerConfig.WEBHOOK_SECRET);
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }
}
