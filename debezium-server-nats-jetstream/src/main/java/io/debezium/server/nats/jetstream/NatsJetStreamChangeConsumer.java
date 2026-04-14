/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.nats.jetstream;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.config.Field;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;
import io.debezium.server.api.DebeziumServerSink;
import io.debezium.server.util.RetryExecutor;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;

/**
 * Implementation of the consumer that delivers the messages into a NATS Jetstream stream.
 *
 * @author Balázs Sipos
 */
@Named("nats-jetstream")
@Dependent
public class NatsJetStreamChangeConsumer extends BaseChangeConsumer
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(NatsJetStreamChangeConsumer.class);

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    private static final String PROP_PREFIX = "debezium.sink.nats-jetstream.";

    private NatsJetStreamChangeConsumerConfig config;

    private Connection nc;
    private JetStream js;
    private RetryExecutor retryExecutor;

    @Inject
    @CustomConsumerBuilder
    Instance<JetStream> customStreamingConnection;

    @PostConstruct
    void connect() {
        final Config mpConfig = ConfigProvider.getConfig();

        // Load configuration
        io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));
        this.config = new NatsJetStreamChangeConsumerConfig(configuration);

        if (customStreamingConnection.isResolvable()) {
            js = customStreamingConnection.get();
            LOGGER.info("Obtained custom configured JetStream '{}'", js);
            return;
        }

        try {
            // Setup NATS connection
            Options.Builder natsOptionsBuilder = new io.nats.client.Options.Builder()
                    .servers(config.getUrl().split(","))
                    .noReconnect();

            if (config.getJwt() != null && config.getSeed() != null) {
                natsOptionsBuilder
                        .authHandler(Nats.staticCredentials(config.getJwt().toCharArray(), config.getSeed().toCharArray()));
            }
            else if (config.getUser() != null && config.getPassword() != null) {
                natsOptionsBuilder.userInfo(config.getUser(), config.getPassword());
            }
            else if (config.getTlsKeyStore() != null && config.getTlsKeyStorePassword() != null && config.getTlsPassword() != null) {
                var ctx = sslAuthContext(config.getTlsKeyStore(), config.getTlsKeyStorePassword(), config.getTlsPassword());
                natsOptionsBuilder.sslContext(ctx);
            }

            nc = Nats.connect(natsOptionsBuilder.build());

            // Creating a basic stream, mostly for testing. If a user wants to configure their stream, it can be done
            // via the nats cli.
            if (config.isCreateStream()) {
                String subjects = config.getSubjects();
                String storage = config.getStorage();
                StorageType storageType = storage.equals("file") ? StorageType.File : StorageType.Memory;

                StreamConfiguration streamConfig = StreamConfiguration.builder()
                        .name(config.getStreamName())
                        .description("The debezium stream, contains messages which are coming from debezium")
                        .subjects(subjects.split(","))
                        .storageType(storageType)
                        .build();

                LOGGER.info("Creating stream with config: {}", streamConfig);

                JetStreamManagement jsm = nc.jetStreamManagement();
                jsm.addStream(streamConfig);
            }

            js = nc.jetStream();
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }

        if (config.isAsyncEnabled()) {
            LOGGER.info("Async publishing enabled with timeout: {}ms", config.getAsyncTimeoutMs());
        }
        else {
            LOGGER.info("Synchronous publishing mode enabled");
        }

        this.retryExecutor = new RetryExecutor(
                config.getSyncMaxRetryAttempts(),
                config.getSyncRetryIntervalMs(),
                config.getSyncRetryMaxIntervalMs(),
                config.getSyncRetryBackoffMultiplier());
    }

    @PreDestroy
    @Override
    public void close() {
        try {
            if (nc != null) {
                nc.close();
                LOGGER.info("NATS connection closed.");
            }
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records,
                            RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        if (!config.isAsyncEnabled() || records.isEmpty()) {
            handleBatchSync(records, committer);
            return;
        }
        handleBatchAsync(records, committer);
    }

    private void handleBatchSync(List<ChangeEvent<Object, Object>> records,
                                 RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        for (ChangeEvent<Object, Object> rec : records) {
            if (rec.value() != null) {
                String subject = streamNameMapper.map(rec.destination());
                LOGGER.trace("Received event @ {} = '{}'", subject, rec.value());
                byte[] recordBytes = getBytes(rec.value());

                final var headers = convertHeaders(rec);
                final var natsHeaders = new Headers();
                if (!headers.isEmpty()) {
                    headers.forEach((key, value) -> {
                        if (value != null) {
                            natsHeaders.add(key, value);
                        }
                    });
                }

                publishWithRetry(subject, natsHeaders, recordBytes);

            }
            committer.markProcessed(rec);
        }
        committer.markBatchFinished();
    }

    private void handleBatchAsync(List<ChangeEvent<Object, Object>> records,
                                  RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        List<CompletableFuture<PublishAck>> futures = records.stream()
                .filter(rec -> rec.value() != null)
                .map(rec -> {
                    String subject = streamNameMapper.map(rec.destination());
                    LOGGER.trace("Received event for async processing @ {} = '{}'", subject, rec.value());
                    byte[] recordBytes = getBytes(rec.value());

                    Headers natsHeaders = new Headers();
                    var headers = convertHeaders(rec);
                    if (!headers.isEmpty()) {
                        headers.forEach((key, value) -> {
                            if (value != null) {
                                natsHeaders.add(key, value);
                            }
                        });
                    }

                    Message msg = NatsMessage.builder()
                            .subject(subject)
                            .headers(natsHeaders)
                            .data(recordBytes)
                            .build();

                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Sending async message to subject: {} (data length: {} bytes)", subject, recordBytes.length);
                    }

                    CompletableFuture<PublishAck> future = js.publishAsync(msg).orTimeout(config.getAsyncTimeoutMs(), TimeUnit.MILLISECONDS);

                    if (LOGGER.isTraceEnabled()) {
                        future.thenAccept(ack -> {
                            LOGGER.trace("Received ACK for subject: {} (stream: {}, seq: {})", subject, ack.getStream(), ack.getSeqno());
                        });
                    }

                    return future;
                })
                .toList();
        if (futures.isEmpty()) {
            for (ChangeEvent<Object, Object> rec : records) {
                committer.markProcessed(rec);
            }
            committer.markBatchFinished();
            return;
        }
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]))
                    .get(config.getAsyncTimeoutMs(), TimeUnit.MILLISECONDS);

            for (ChangeEvent<Object, Object> rec : records) {
                committer.markProcessed(rec);
            }
            committer.markBatchFinished();
            LOGGER.debug("Successfully published batch of {} messages", records.size());
        }
        catch (TimeoutException e) {
            LOGGER.error("Timeout waiting for publish acknowledgments after {} ms", config.getAsyncTimeoutMs());
            throw new DebeziumException("Timeout waiting for publish acknowledgments", e);
        }
        catch (ExecutionException e) {
            LOGGER.error("Failed to publish batch", e.getCause());
            throw new DebeziumException("Failed to publish batch", e.getCause());
        }
    }

    /**
     * Publishes a message to NATS JetStream with retry logic.
     *
     * @param subject The subject to publish to
     * @param headers The NATS headers
     * @param data The message payload
     * @throws InterruptedException if interrupted during retry sleep
     * @throws DebeziumException if max retries exceeded or non-retryable error occurs
     */
    private void publishWithRetry(String subject, Headers headers, byte[] data) throws InterruptedException {

        retryExecutor.executeWithRetry(
                () -> {
                    if (!headers.isEmpty()) {
                        js.publish(subject, headers, data);
                    }
                    else {
                        js.publish(subject, data);
                    }
                    return null;
                },
                this::isRetryableNATSException,
                "publish to NATS:" + subject);
    }

    /**
     * Determines if a NATS exception is retryable based on its type and error codes.
     *
     * @param e Exception to evaluate
     * @return true if retryable, false otherwise
     */
    private boolean isRetryableNATSException(Exception e) {
        // Handle JetStreamApiException with error codes
        if (e instanceof JetStreamApiException jsEx) {
            int apiErrorCode = jsEx.getApiErrorCode();
            // http style codes io.nats.client.api.Error
            int errCode = jsEx.getErrorCode();

            // Check specific JetStream API error codes in NATS repository
            // https://github.com/nats-io/nats-server/blob/main/server/errors.json

            // Non-retryable configuration/client errors
            Set<Integer> nonRetryableApiCodes = Set.of(
                    10059, // JSStreamNotFoundErr
                    10003, // JSBadRequestErr
                    10039, // JSNotEnabledForAccountErr
                    10076 // JSNotEnabledEre
            );

            if (nonRetryableApiCodes.contains(apiErrorCode)) {
                return false;
            }

            // Retryable transient errors
            Set<Integer> retryableApiCodes = Set.of(
                    10008, // JSClusterNotAvailErr
                    10023, // JSInsufficientResourcesErr
                    10077, // JSStreamStoreFailedF
                    10167, // JSStreamTooManyRequests - when too many requests are made in a short time can be retried after a delay
                    10122 // JSStreamMaxStreamBytesExceeded - when stream is full but can be retried after consumers have processed messages
            );

            if (retryableApiCodes.contains(apiErrorCode)) {
                return true;
            }

            // Typically 503 error codes are retryable
            return errCode > 500;
        }

        // Transport issues generally indicate transient problems thus retryable
        if (e instanceof IOException) {
            return true;
        }

        // Unknown error possibly transient, treat as retryable
        LOGGER.warn("Unknown exception type, treating as retryable: {}", e.getClass().getName());
        return true;
    }

    private static SSLContext sslAuthContext(String keystorePath, String keystorePassword,
                                             String password)
            throws Exception {

        var keystore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (var in = new BufferedInputStream(new FileInputStream(keystorePath))) {
            keystore.load(in, keystorePassword.toCharArray());
        }

        var kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keystore, password.toCharArray());

        var tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(keystore);

        var ctx = SSLContext.getInstance(Options.DEFAULT_SSL_PROTOCOL);
        ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());

        return ctx;
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                NatsJetStreamChangeConsumerConfig.URL,
                NatsJetStreamChangeConsumerConfig.STREAM_NAME,
                NatsJetStreamChangeConsumerConfig.CREATE_STREAM,
                NatsJetStreamChangeConsumerConfig.SUBJECTS,
                NatsJetStreamChangeConsumerConfig.STORAGE,
                NatsJetStreamChangeConsumerConfig.AUTH_JWT,
                NatsJetStreamChangeConsumerConfig.AUTH_SEED,
                NatsJetStreamChangeConsumerConfig.AUTH_USER,
                NatsJetStreamChangeConsumerConfig.AUTH_PASSWORD,
                NatsJetStreamChangeConsumerConfig.AUTH_TLS_KEYSTORE,
                NatsJetStreamChangeConsumerConfig.AUTH_TLS_KEYSTORE_PASSWORD,
                NatsJetStreamChangeConsumerConfig.AUTH_TLS_PASSWORD,
                NatsJetStreamChangeConsumerConfig.ASYNC_ENABLED,
                NatsJetStreamChangeConsumerConfig.ASYNC_TIMEOUT_MS,
                NatsJetStreamChangeConsumerConfig.SYNC_RETRIES,
                NatsJetStreamChangeConsumerConfig.SYNC_RETRY_INTERVAL_MS,
                NatsJetStreamChangeConsumerConfig.SYNC_RETRY_MAX_INTERVAL_MS,
                NatsJetStreamChangeConsumerConfig.SYNC_RETRY_BACKOFF_MULTIPLIER);
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }
}
