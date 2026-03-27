/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.nats.jetstream;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link NatsJetStreamChangeConsumer}.
 */
public class NatsJetStreamChangeConsumerConfig {

    public static final Field URL = Field.create("url")
            .withDisplayName("URL")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("NATS server URL(s), comma-separated.");

    public static final Field STREAM_NAME = Field.create("stream-name")
            .withDisplayName("Stream Name")
            .withType(ConfigDef.Type.STRING)
            .withDefault("DebeziumStream")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("NATS JetStream stream name.");

    public static final Field CREATE_STREAM = Field.create("create-stream")
            .withDisplayName("Create Stream")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Whether to create the stream if it doesn't exist.");

    public static final Field SUBJECTS = Field.create("subjects")
            .withDisplayName("Subjects")
            .withType(ConfigDef.Type.STRING)
            .withDefault("*.*.*")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Subjects for the stream, comma-separated.");

    public static final Field STORAGE = Field.create("storage")
            .withDisplayName("Storage Type")
            .withType(ConfigDef.Type.STRING)
            .withDefault("memory")
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Storage type for the stream (memory or file).");

    public static final Field AUTH_JWT = Field.create("auth.jwt")
            .withDisplayName("JWT Token")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("JWT token for authentication.");

    public static final Field AUTH_SEED = Field.create("auth.seed")
            .withDisplayName("Seed")
            .withType(ConfigDef.Type.PASSWORD)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Seed for JWT authentication.");

    public static final Field AUTH_USER = Field.create("auth.user")
            .withDisplayName("User")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Username for basic authentication.");

    public static final Field AUTH_PASSWORD = Field.create("auth.password")
            .withDisplayName("Password")
            .withType(ConfigDef.Type.PASSWORD)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Password for basic authentication.");

    public static final Field AUTH_TLS_KEYSTORE = Field.create("auth.tls.keystore")
            .withDisplayName("TLS Keystore Path")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Path to TLS keystore.");

    public static final Field AUTH_TLS_KEYSTORE_PASSWORD = Field.create("auth.tls.keystore.password")
            .withDisplayName("TLS Keystore Password")
            .withType(ConfigDef.Type.PASSWORD)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Password for TLS keystore.");

    public static final Field AUTH_TLS_PASSWORD = Field.create("auth.tls.password")
            .withDisplayName("TLS Password")
            .withType(ConfigDef.Type.PASSWORD)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Password for TLS authentication.");

    public static final Field ASYNC_ENABLED = Field.create("async.enabled")
            .withDisplayName("Async Publishing Enabled")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(true)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Enable asynchronous message publishing.");

    public static final Field ASYNC_TIMEOUT_MS = Field.create("async.timeout.ms")
            .withDisplayName("Async Timeout (ms)")
            .withType(ConfigDef.Type.LONG)
            .withDefault(5000L)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Timeout for asynchronous publishing in milliseconds.");

    public static final Field SYNC_RETRIES = Field.create("sync.retries")
            .withDisplayName("Sync Retries")
            .withType(ConfigDef.Type.INT)
            .withDefault(5)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum number of retry attempts for synchronous publishing.");

    public static final Field SYNC_RETRY_INTERVAL_MS = Field.create("sync.retry.interval.ms")
            .withDisplayName("Sync Retry Interval (ms)")
            .withType(ConfigDef.Type.LONG)
            .withDefault(1000L)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Initial retry interval in milliseconds.");

    public static final Field SYNC_RETRY_MAX_INTERVAL_MS = Field.create("sync.retry.max.interval.ms")
            .withDisplayName("Sync Retry Max Interval (ms)")
            .withType(ConfigDef.Type.LONG)
            .withDefault(60000L)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum retry interval in milliseconds.");

    public static final Field SYNC_RETRY_BACKOFF_MULTIPLIER = Field.create("sync.retry.backoff.multiplier")
            .withDisplayName("Sync Retry Backoff Multiplier")
            .withType(ConfigDef.Type.DOUBLE)
            .withDefault("2.0")
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Backoff multiplier for retry intervals.");

    // Instance fields
    private String url;
    private String streamName;
    private boolean createStream;
    private String subjects;
    private String storage;
    private String jwt;
    private String seed;
    private String user;
    private String password;
    private String tlsKeyStore;
    private String tlsKeyStorePassword;
    private String tlsPassword;
    private boolean asyncEnabled;
    private long asyncTimeoutMs;
    private int syncMaxRetryAttempts;
    private long syncRetryIntervalMs;
    private long syncRetryMaxIntervalMs;
    private double syncRetryBackoffMultiplier;

    public NatsJetStreamChangeConsumerConfig(Configuration config) {
        init(config);
    }

    protected void init(Configuration config) {
        url = config.getString(URL);
        streamName = config.getString(STREAM_NAME);
        createStream = config.getBoolean(CREATE_STREAM, false);
        subjects = config.getString(SUBJECTS);
        storage = config.getString(STORAGE);
        jwt = config.getString(AUTH_JWT);
        seed = config.getString(AUTH_SEED);
        user = config.getString(AUTH_USER);
        password = config.getString(AUTH_PASSWORD);
        tlsKeyStore = config.getString(AUTH_TLS_KEYSTORE);
        tlsKeyStorePassword = config.getString(AUTH_TLS_KEYSTORE_PASSWORD);
        tlsPassword = config.getString(AUTH_TLS_PASSWORD);
        asyncEnabled = config.getBoolean(ASYNC_ENABLED, true);
        asyncTimeoutMs = config.getLong(ASYNC_TIMEOUT_MS, 5000L);
        syncMaxRetryAttempts = config.getInteger(SYNC_RETRIES, 5);
        syncRetryIntervalMs = config.getLong(SYNC_RETRY_INTERVAL_MS, 1000L);
        syncRetryMaxIntervalMs = config.getLong(SYNC_RETRY_MAX_INTERVAL_MS, 60000L);
        syncRetryBackoffMultiplier = Double.parseDouble(config.getString(SYNC_RETRY_BACKOFF_MULTIPLIER, "2.0"));
    }

    public String getUrl() {
        return url;
    }

    public String getStreamName() {
        return streamName;
    }

    public boolean isCreateStream() {
        return createStream;
    }

    public String getSubjects() {
        return subjects;
    }

    public String getStorage() {
        return storage;
    }

    public String getJwt() {
        return jwt;
    }

    public String getSeed() {
        return seed;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getTlsKeyStore() {
        return tlsKeyStore;
    }

    public String getTlsKeyStorePassword() {
        return tlsKeyStorePassword;
    }

    public String getTlsPassword() {
        return tlsPassword;
    }

    public boolean isAsyncEnabled() {
        return asyncEnabled;
    }

    public long getAsyncTimeoutMs() {
        return asyncTimeoutMs;
    }

    public int getSyncMaxRetryAttempts() {
        return syncMaxRetryAttempts;
    }

    public long getSyncRetryIntervalMs() {
        return syncRetryIntervalMs;
    }

    public long getSyncRetryMaxIntervalMs() {
        return syncRetryMaxIntervalMs;
    }

    public double getSyncRetryBackoffMultiplier() {
        return syncRetryBackoffMultiplier;
    }
}
