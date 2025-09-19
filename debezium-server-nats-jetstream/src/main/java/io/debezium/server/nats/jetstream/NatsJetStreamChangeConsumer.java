/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.nats.jetstream;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.List;
import java.util.Optional;
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
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;
import io.nats.client.Connection;
import io.nats.client.JetStream;
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
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NatsJetStreamChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.nats-jetstream.";
    private static final String PROP_URL = PROP_PREFIX + "url";
    private static final String PROP_CREATE_STREAM = PROP_PREFIX + "create-stream";
    private static final String PROP_SUBJECTS = PROP_PREFIX + "subjects";
    private static final String PROP_STORAGE = PROP_PREFIX + "storage";

    private static final String PROP_AUTH_JWT = PROP_PREFIX + "auth.jwt";
    private static final String PROP_AUTH_SEED = PROP_PREFIX + "auth.seed";
    private static final String PROP_AUTH_USER = PROP_PREFIX + "auth.user";
    private static final String PROP_AUTH_PASSWORD = PROP_PREFIX + "auth.password";
    private static final String PROP_AUTH_TLS_KEYSTORE = PROP_PREFIX + "auth.tls.keystore";
    private static final String PROP_AUTH_TLS_KEYSTORE_PASSWORD = PROP_PREFIX + "auth.tls.keystore.password";
    private static final String PROP_AUTH_TLS_PASSWORD = PROP_PREFIX + "auth.tls.password";

    private static final String PROP_ASYNC_ENABLED = PROP_PREFIX + "async.enabled";
    private static final String PROP_ASYNC_TIMEOUT_MS = PROP_PREFIX + "async.timeout-ms";

    private Connection nc;
    private JetStream js;

    @ConfigProperty(name = PROP_CREATE_STREAM, defaultValue = "false")
    boolean createStream;

    @ConfigProperty(name = PROP_AUTH_JWT)
    Optional<String> jwt;

    @ConfigProperty(name = PROP_AUTH_SEED)
    Optional<String> seed;

    @ConfigProperty(name = PROP_AUTH_USER)
    Optional<String> user;

    @ConfigProperty(name = PROP_AUTH_PASSWORD)
    Optional<String> password;

    @ConfigProperty(name = PROP_AUTH_TLS_KEYSTORE)
    Optional<String> tlsKeyStore;

    @ConfigProperty(name = PROP_AUTH_TLS_KEYSTORE_PASSWORD)
    Optional<String> tlsKeyStorePassword;

    @ConfigProperty(name = PROP_AUTH_TLS_PASSWORD)
    Optional<String> tlsPassword;

    @ConfigProperty(name = PROP_ASYNC_ENABLED, defaultValue = "true")
    boolean asyncEnabled;

    @ConfigProperty(name = PROP_ASYNC_TIMEOUT_MS, defaultValue = "5000")
    long asyncTimeoutMs;

    @Inject
    @CustomConsumerBuilder
    Instance<JetStream> customStreamingConnection;

    @PostConstruct
    void connect() {
        // Read config
        final Config config = ConfigProvider.getConfig();
        String url = config.getValue(PROP_URL, String.class);

        if (customStreamingConnection.isResolvable()) {
            js = customStreamingConnection.get();
            LOGGER.info("Obtained custom configured JetStream '{}'", js);
            return;
        }

        try {
            // Setup NATS connection
            Options.Builder natsOptionsBuilder = new io.nats.client.Options.Builder()
                    .servers(url.split(","))
                    .noReconnect();

            if (jwt.isPresent() && seed.isPresent()) {
                natsOptionsBuilder
                        .authHandler(Nats.staticCredentials(jwt.get().toCharArray(), seed.get().toCharArray()));
            }
            else if (user.isPresent() && password.isPresent()) {
                natsOptionsBuilder.userInfo(user.get(), password.get());
            }
            else if (tlsKeyStore.isPresent() && tlsKeyStorePassword.isPresent() && tlsPassword.isPresent()) {
                var ctx = sslAuthContext(tlsKeyStore.get(), tlsKeyStorePassword.get(), tlsPassword.get());
                natsOptionsBuilder.sslContext(ctx);
            }

            nc = Nats.connect(natsOptionsBuilder.build());

            // Creating a basic stream, mostly for testing. If a user wants to configure their stream, it can be done
            // via the nats cli.
            if (createStream) {
                String subjects = config.getOptionalValue(PROP_SUBJECTS, String.class).orElse("*.*.*");
                String storage = config.getOptionalValue(PROP_STORAGE, String.class).orElse("memory");
                StorageType storageType = storage.equals("file") ? StorageType.File : StorageType.Memory;

                StreamConfiguration streamConfig = StreamConfiguration.builder()
                        .name("DebeziumStream")
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

        if (asyncEnabled) {
            LOGGER.info("Async publishing enabled with timeout: {}ms", asyncTimeoutMs);
        }
        else {
            LOGGER.info("Synchronous publishing mode enabled");
        }
    }

    @PreDestroy
    void close() {
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
        if (!asyncEnabled || records.isEmpty()) {
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

                try {
                    if (!natsHeaders.isEmpty()) {
                        js.publish(subject, natsHeaders, recordBytes);
                    }
                    else {
                        js.publish(subject, recordBytes);
                    }
                }
                catch (Exception e) {
                    throw new DebeziumException(e);
                }
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

                    LOGGER.trace("Sending async message to subject: {} (data length: {} bytes)", subject, recordBytes.length);

                    CompletableFuture<PublishAck> future = js.publishAsync(msg).orTimeout(asyncTimeoutMs, TimeUnit.MILLISECONDS);

                    future.thenAccept(ack -> {
                        LOGGER.trace("Received ACK for subject: {} (stream: {}, seq: {})", subject, ack.getStream(), ack.getSeqno());
                    });
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
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(asyncTimeoutMs, TimeUnit.MILLISECONDS);

            for (ChangeEvent<Object, Object> rec : records) {
                committer.markProcessed(rec);
            }
            committer.markBatchFinished();
            LOGGER.debug("Successfully published batch of {} messages", records.size());
        }
        catch (TimeoutException e) {
            LOGGER.error("Timeout waiting for publish acknowledgments after {}ms", asyncTimeoutMs);
            throw new DebeziumException("Timeout waiting for publish acknowledgments", e);
        }
        catch (ExecutionException e) {
            LOGGER.error("Failed to publish batch", e.getCause());
            throw new DebeziumException("Failed to publish batch", e.getCause());
        }
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
}
