/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http;

import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_OK;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.http.jwt.JWTAuthenticatorBuilder;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Implementation of the consumer that delivers the messages to an HTTP Webhook destination.
 *
 * @author Chris Baumbauer
 */
@Named("http")
@Dependent
public class HttpChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpChangeConsumer.class);

    public static final String PROP_PREFIX = "debezium.sink.http.";
    public static final String PROP_WEBHOOK_URL = "url";
    public static final String PROP_CLIENT_TIMEOUT = "timeout.ms";
    public static final String PROP_RETRIES = "retries";
    public static final String PROP_RETRY_INTERVAL = "retry.interval.ms";
    public static final String PROP_HEADERS_ENCODE_BASE64 = "headers.encode.base64";
    public static final String PROP_HEADERS_PREFIX = "headers.prefix";

    public static final String PROP_AUTHENTICATION_PREFIX = PROP_PREFIX + "authentication.";
    public static final String PROP_AUTHENTICATION_TYPE = "type";
    public static final String JWT_AUTHENTICATION = "jwt";

    private static final Long HTTP_TIMEOUT = Integer.toUnsignedLong(60000); // Default to 60s
    private static final int DEFAULT_RETRIES = 5;
    private static final Long RETRY_INTERVAL = Integer.toUnsignedLong(1_000); // Default to 1s
    private static final String DEFAULT_HEADERS_PREFIX = "X-DEBEZIUM-";

    private static Duration timeoutDuration;
    private static int retries;
    private static Duration retryInterval;
    private boolean base64EncodeHeaders = true;
    private String headersPrefix = DEFAULT_HEADERS_PREFIX;

    private HttpClient client;
    private HttpRequest.Builder requestBuilder;

    // not null if using authentication; null otherwise
    private Authenticator authenticator;

    // If this is running as a Knative object, then expect the sink URL to be located in `K_SINK`
    // as per https://knative.dev/development/eventing/custom-event-source/sinkbinding/
    @PostConstruct
    void connect() throws URISyntaxException {
        final Config config = ConfigProvider.getConfig();
        initWithConfig(config);
    }

    @VisibleForTesting
    void initWithConfig(Config config) throws URISyntaxException {
        String sinkUrl;
        String contentType;

        client = HttpClient.newHttpClient();
        String sink = System.getenv("K_SINK");
        timeoutDuration = Duration.ofMillis(HTTP_TIMEOUT);
        retries = DEFAULT_RETRIES;
        retryInterval = Duration.ofMillis(RETRY_INTERVAL);

        if (sink != null) {
            sinkUrl = sink;
        }
        else {
            sinkUrl = config.getValue(PROP_PREFIX + PROP_WEBHOOK_URL, String.class);
        }

        config.getOptionalValue(PROP_PREFIX + PROP_CLIENT_TIMEOUT, String.class)
                .ifPresent(t -> timeoutDuration = Duration.ofMillis(Long.parseLong(t)));

        config.getOptionalValue(PROP_PREFIX + PROP_RETRIES, String.class)
                .ifPresent(n -> retries = Integer.parseInt(n));

        config.getOptionalValue(PROP_PREFIX + PROP_RETRY_INTERVAL, String.class)
                .ifPresent(t -> retryInterval = Duration.ofMillis(Long.parseLong(t)));

        config.getOptionalValue(PROP_PREFIX + PROP_HEADERS_PREFIX, String.class)
                .ifPresent(p -> headersPrefix = p);

        config.getOptionalValue(PROP_PREFIX + PROP_HEADERS_ENCODE_BASE64, Boolean.class)
                .ifPresent(b -> base64EncodeHeaders = b);

        switch (config.getValue("debezium.format.value", String.class)) {
            case "avro":
                contentType = "avro/bytes";
                break;
            case "cloudevents":
                contentType = "application/cloudevents+json";
                break;
            default:
                // Note: will default to JSON if it cannot be determined, but should not reach this point
                contentType = "application/json";
        }

        // Need to be able to throw an exception
        // so not using ifPresent() syntax
        Optional<String> authenticationType = config.getOptionalValue(PROP_AUTHENTICATION_PREFIX + PROP_AUTHENTICATION_TYPE, String.class);
        if (authenticationType.isPresent()) {
            String t = authenticationType.get();
            if (t.equalsIgnoreCase(JWT_AUTHENTICATION)) {
                JWTAuthenticatorBuilder builder = JWTAuthenticatorBuilder.fromConfig(config, PROP_AUTHENTICATION_PREFIX);
                authenticator = builder.build();
            }
            else {
                throw new DebeziumException("Unknown value '" + t + "' encountered for property " + PROP_AUTHENTICATION_PREFIX + PROP_AUTHENTICATION_TYPE);
            }
        }

        LOGGER.info("Using http content-type type {}", contentType);
        LOGGER.info("Using sink URL: {}", sinkUrl);
        requestBuilder = HttpRequest.newBuilder(new URI(sinkUrl)).timeout(timeoutDuration);
        requestBuilder.setHeader("content-type", contentType);
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);

            if (record.value() != null) {
                int attempts = 0;
                while (!recordSent(record)) {
                    attempts++;
                    if (attempts >= retries) {
                        throw new DebeziumException("Exceeded maximum number of attempts to publish event " + record);
                    }
                    Metronome.sleeper(retryInterval, Clock.SYSTEM).pause();
                }
                committer.markProcessed(record);
            }
        }

        committer.markBatchFinished();
    }

    private boolean recordSent(ChangeEvent<Object, Object> record) throws InterruptedException {
        boolean sent = false;
        HttpResponse<String> r;

        HttpRequest.Builder requestBuilder = generateRequest(record);

        try {
            if (authenticator != null) {
                if (!authenticator.authenticate()) {
                    throw new DebeziumException("Failed to authenticate successfully.  Cannot continue.");
                }
                authenticator.setAuthorizationHeader(requestBuilder);
            }

            HttpRequest request = requestBuilder.build();

            r = client.send(request, HttpResponse.BodyHandlers.ofString());
        }
        catch (IOException ioe) {
            throw new InterruptedException(ioe.toString());
        }

        if ((r.statusCode() == HTTP_OK) || (r.statusCode() == HTTP_NO_CONTENT) || (r.statusCode() == HTTP_ACCEPTED)) {
            sent = true;
        }
        else {
            LOGGER.info("Failed to publish event: " + r.body());
        }

        return sent;
    }

    @VisibleForTesting
    HttpRequest.Builder generateRequest(ChangeEvent<Object, Object> record) {
        String value = (String) record.value();
        HttpRequest.Builder builder = requestBuilder.POST(HttpRequest.BodyPublishers.ofString(value));

        Map<String, String> headers = convertHeaders(record);

        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String headerValue = entry.getValue();
            if (base64EncodeHeaders) {
                headerValue = Base64.getEncoder().encodeToString(headerValue.getBytes(StandardCharsets.UTF_8));
            }
            builder.header(headersPrefix + entry.getKey().toUpperCase(Locale.ROOT), headerValue);
        }

        return builder;
    }
}
