/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rabbitmq;

import java.time.Duration;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link RabbitMqStreamNativeChangeConsumer}.
 */
public class RabbitMqStreamNativeChangeConsumerConfig {

    protected static final String STORE_TYPE_PEM = "PEM";
    protected static final String STORE_TYPE_PKCS12 = "PKCS12";
    protected static final String STORE_TYPE_JKS = "JKS";

    public static final Field CONNECTION_HOST = Field.create("connection.host")
            .withDisplayName("Connection Host (deprecated)")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Connection host (deprecated, use 'host' instead).");

    public static final Field CONNECTION_PORT = Field.create("connection.port")
            .withDisplayName("Connection Port (deprecated)")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Connection port (deprecated, use 'port' instead).");

    public static final Field HOST = Field.create("host")
            .withDisplayName("Host")
            .withType(ConfigDef.Type.STRING)
            .withDefault("localhost")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("RabbitMQ Stream host.");

    public static final Field PORT = Field.create("port")
            .withDisplayName("Port")
            .withType(ConfigDef.Type.INT)
            .withDefault(5552)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("RabbitMQ Stream port.");

    public static final Field USERNAME = Field.create("username")
            .withDisplayName("Username")
            .withType(ConfigDef.Type.STRING)
            .withDefault("guest")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("RabbitMQ username.");

    public static final Field PASSWORD = Field.create("password")
            .withDisplayName("Password")
            .withType(ConfigDef.Type.PASSWORD)
            .withDefault("guest")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("RabbitMQ password.");

    public static final Field VIRTUAL_HOST = Field.create("virtualHost")
            .withDisplayName("Virtual Host")
            .withType(ConfigDef.Type.STRING)
            .withDefault("/")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("RabbitMQ virtual host.");

    public static final Field TLS_ENABLE = Field.create("tls.enable")
            .withDisplayName("TLS Enable")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Enable TLS connection.");

    public static final Field KEY_STORE_TYPE = Field.create("tls.keyStore.type")
            .withDisplayName("Key Store Type")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Type of the client key store: PEM, PKCS12, or JKS. Required for mTLS.");

    public static final Field KEY_STORE_CERTIFICATE_FILE_PATH = Field.create("tls.keyStore.certificateFilePath")
            .withDisplayName("Key Store Certificate File Path")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Path to the client certificate PEM file. Required together with tls.keyStore.keyFilePath when tls.keyStore.type is PEM.");

    public static final Field KEY_STORE_KEY_FILE_PATH = Field.create("tls.keyStore.keyFilePath")
            .withDisplayName("Key Store Key File Path")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Path to the client private key PEM file. Required together with tls.keyStore.certificateFilePath when tls.keyStore.type is PEM.");

    public static final Field KEY_STORE_FILE_PATH = Field.create("tls.keyStore.filePath")
            .withDisplayName("Key Store File Path")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Path to the client key store file. Required when tls.keyStore.type is PKCS12 or JKS.");

    public static final Field KEY_STORE_PASSWORD = Field.create("tls.keyStore.password")
            .withDisplayName("Key Store Password")
            .withType(ConfigDef.Type.PASSWORD)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Password for the key store (PKCS12/JKS), or passphrase for the private key file (PEM), if any.");

    public static final Field TRUST_STORE_TYPE = Field.create("tls.trustStore.type")
            .withDisplayName("Trust Store Type")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Type of the trust store: PEM, PKCS12, or JKS. Required to trust a private/internal CA.");

    public static final Field TRUST_STORE_FILE_PATH = Field.create("tls.trustStore.filePath")
            .withDisplayName("Trust Store File Path")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Path to the trust store file (a CA bundle for PEM, or a key store file for PKCS12/JKS). Required when tls.trustStore.type is set.");

    public static final Field TRUST_STORE_PASSWORD = Field.create("tls.trustStore.password")
            .withDisplayName("Trust Store Password")
            .withType(ConfigDef.Type.PASSWORD)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Password for the trust store. Only relevant when tls.trustStore.type is PKCS12 or JKS.");

    public static final Field TLS_VERIFY_HOSTNAME = Field.create("tls.verify.hostname")
            .withDisplayName("TLS Verify Hostname")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Verify that the server's certificate matches the host being connected to. Defaults to false to preserve prior behavior.");

    public static final Field RPC_TIMEOUT = Field.create("rpcTimeout")
            .withDisplayName("RPC Timeout (seconds)")
            .withType(ConfigDef.Type.INT)
            .withDefault(10)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("RPC timeout in seconds.");

    public static final Field MAX_PRODUCERS_BY_CONNECTION = Field.create("maxProducersByConnection")
            .withDisplayName("Max Producers By Connection")
            .withType(ConfigDef.Type.INT)
            .withDefault(256)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum number of producers per connection.");

    public static final Field MAX_TRACKING_CONSUMERS_BY_CONNECTION = Field.create("maxTrackingConsumersByConnection")
            .withDisplayName("Max Tracking Consumers By Connection")
            .withType(ConfigDef.Type.INT)
            .withDefault(50)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum number of tracking consumers per connection.");

    public static final Field MAX_CONSUMERS_BY_CONNECTION = Field.create("maxConsumersByConnection")
            .withDisplayName("Max Consumers By Connection")
            .withType(ConfigDef.Type.INT)
            .withDefault(256)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum number of consumers per connection.");

    public static final Field REQUESTED_HEARTBEAT = Field.create("requestedHeartbeat")
            .withDisplayName("Requested Heartbeat (seconds)")
            .withType(ConfigDef.Type.INT)
            .withDefault(60)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Requested heartbeat interval in seconds.");

    public static final Field REQUESTED_MAX_FRAME_SIZE = Field.create("requestedMaxFrameSize")
            .withDisplayName("Requested Max Frame Size")
            .withType(ConfigDef.Type.INT)
            .withDefault(0)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Requested maximum frame size.");

    public static final Field ID = Field.create("id")
            .withDisplayName("ID")
            .withType(ConfigDef.Type.STRING)
            .withDefault("rabbitmq-stream")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Client ID.");

    public static final Field STREAM = Field.create("stream")
            .withDisplayName("Stream")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Stream name.");

    public static final Field STREAM_MAX_AGE = Field.create("stream.maxAge")
            .withDisplayName("Stream Max Age")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Maximum age for stream messages.");

    public static final Field STREAM_MAX_LENGTH = Field.create("stream.maxLength")
            .withDisplayName("Stream Max Length")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Maximum length for stream in bytes.");

    public static final Field STREAM_MAX_SEGMENT_SIZE = Field.create("stream.maxSegmentSize")
            .withDisplayName("Stream Max Segment Size")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Maximum segment size for stream in bytes.");

    public static final Field SUPER_STREAM_ENABLE = Field.create("superStream.enable")
            .withDisplayName("Super Stream Enable")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Enable super stream.");

    public static final Field SUPER_STREAM_PARTITIONS = Field.create("superStream.partitions")
            .withDisplayName("Super Stream Partitions")
            .withType(ConfigDef.Type.INT)
            .withDefault(3)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Number of partitions for super stream.");

    public static final Field SUPER_STREAM_BINDING_KEYS = Field.create("superStream.bindingKeys")
            .withDisplayName("Super Stream Binding Keys")
            .withType(ConfigDef.Type.LIST)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Binding keys for super stream.");

    public static final Field PRODUCER_NAME = Field.create("producer.name")
            .withDisplayName("Producer Name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Producer name.");

    public static final Field PRODUCER_FILTER_VALUE = Field.create("producer.filterValue")
            .withDisplayName("Producer Filter Value")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Producer filter value.");

    public static final Field PRODUCER_BATCH_SIZE = Field.create("producer.batchSize")
            .withDisplayName("Producer Batch Size")
            .withType(ConfigDef.Type.INT)
            .withDefault(100)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Producer batch size.");

    public static final Field PRODUCER_SUB_ENTRY_SIZE = Field.create("producer.subEntrySize")
            .withDisplayName("Producer Sub Entry Size")
            .withType(ConfigDef.Type.INT)
            .withDefault(1)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Producer sub-entry size.");

    public static final Field PRODUCER_MAX_UNCONFIRMED_MESSAGES = Field.create("producer.maxUnconfirmedMessages")
            .withDisplayName("Producer Max Unconfirmed Messages")
            .withType(ConfigDef.Type.INT)
            .withDefault(10000)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum number of unconfirmed messages.");

    public static final Field PRODUCER_BATCH_PUBLISHING_DELAY = Field.create("producer.batchPublishingDelay")
            .withDisplayName("Producer Batch Publishing Delay (ms)")
            .withType(ConfigDef.Type.INT)
            .withDefault(100)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Producer batch publishing delay in milliseconds.");

    public static final Field PRODUCER_CONFIRM_TIMEOUT = Field.create("producer.confirmTimeout")
            .withDisplayName("Producer Confirm Timeout (seconds)")
            .withType(ConfigDef.Type.INT)
            .withDefault(30)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Producer confirm timeout in seconds.");

    public static final Field PRODUCER_ENQUEUE_TIMEOUT = Field.create("producer.enqueueTimeout")
            .withDisplayName("Producer Enqueue Timeout (seconds)")
            .withType(ConfigDef.Type.INT)
            .withDefault(10)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Producer enqueue timeout in seconds.");

    public static final Field BATCH_CONFIRM_TIMEOUT = Field.create("batchConfirmTimeout")
            .withDisplayName("Batch Confirm Timeout (seconds)")
            .withType(ConfigDef.Type.INT)
            .withDefault(30)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Batch confirm timeout in seconds.");

    public static final Field NULL_VALUE = Field.create("null.value")
            .withDisplayName("Null Value")
            .withType(ConfigDef.Type.STRING)
            .withDefault("default")
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Value to use when record value is null.");

    // Instance fields
    private String connectionHost;
    private Integer connectionPort;
    private String host;
    private int port;
    private String username;
    private String password;
    private String virtualHost;
    private boolean tlsEnable;
    private String keyStoreType;
    private String keyStoreCertificateFilePath;
    private String keyStoreKeyFilePath;
    private String keyStoreFilePath;
    private String keyStorePassword;
    private String trustStoreType;
    private String trustStoreFilePath;
    private String trustStorePassword;
    private boolean tlsVerifyHostname;
    private int rpcTimeout;
    private int maxProducersByConnection;
    private int maxTrackingConsumersByConnection;
    private int maxConsumersByConnection;
    private int requestedHeartbeat;
    private int requestedMaxFrameSize;
    private String id;
    private String stream;
    private Duration streamMaxAge;
    private String streamMaxLength;
    private String streamMaxSegmentSize;
    private boolean superStreamEnable;
    private int superStreamPartitions;
    private String[] superStreamBindingKeys;
    private String producerName;
    private String producerFilterValue;
    private int producerBatchSize;
    private int producerSubEntrySize;
    private int producerMaxUnconfirmedMessages;
    private int producerBatchPublishingDelay;
    private int producerConfirmTimeout;
    private int producerEnqueueTimeout;
    private int batchConfirmTimeout;
    private String nullValue;

    public RabbitMqStreamNativeChangeConsumerConfig(Configuration config) {
        init(config);
    }

    protected void init(Configuration config) {
        connectionHost = config.getString(CONNECTION_HOST);
        // CONNECTION_PORT is deprecated and optional - handle null case
        String connectionPortStr = config.getString(CONNECTION_PORT);
        connectionPort = (connectionPortStr != null) ? Integer.valueOf(connectionPortStr) : null;
        host = config.getString(HOST);
        port = config.getInteger(PORT, 5552);
        username = config.getString(USERNAME);
        password = config.getString(PASSWORD);
        virtualHost = config.getString(VIRTUAL_HOST);
        tlsEnable = config.getBoolean(TLS_ENABLE, false);
        keyStoreType = config.getString(KEY_STORE_TYPE);
        keyStoreCertificateFilePath = config.getString(KEY_STORE_CERTIFICATE_FILE_PATH);
        keyStoreKeyFilePath = config.getString(KEY_STORE_KEY_FILE_PATH);
        keyStoreFilePath = config.getString(KEY_STORE_FILE_PATH);
        keyStorePassword = config.getString(KEY_STORE_PASSWORD);
        trustStoreType = config.getString(TRUST_STORE_TYPE);
        trustStoreFilePath = config.getString(TRUST_STORE_FILE_PATH);
        trustStorePassword = config.getString(TRUST_STORE_PASSWORD);
        tlsVerifyHostname = config.getBoolean(TLS_VERIFY_HOSTNAME, false);
        rpcTimeout = config.getInteger(RPC_TIMEOUT, 10);
        maxProducersByConnection = config.getInteger(MAX_PRODUCERS_BY_CONNECTION, 256);
        maxTrackingConsumersByConnection = config.getInteger(MAX_TRACKING_CONSUMERS_BY_CONNECTION, 50);
        maxConsumersByConnection = config.getInteger(MAX_CONSUMERS_BY_CONNECTION, 256);
        requestedHeartbeat = config.getInteger(REQUESTED_HEARTBEAT, 60);
        requestedMaxFrameSize = config.getInteger(REQUESTED_MAX_FRAME_SIZE, 0);
        id = config.getString(ID);
        stream = config.getString(STREAM);

        // Parse Duration from string if present
        String maxAgeStr = config.getString(STREAM_MAX_AGE);
        if (maxAgeStr != null) {
            streamMaxAge = Duration.parse(maxAgeStr);
        }

        streamMaxLength = config.getString(STREAM_MAX_LENGTH);
        streamMaxSegmentSize = config.getString(STREAM_MAX_SEGMENT_SIZE);
        superStreamEnable = config.getBoolean(SUPER_STREAM_ENABLE, false);
        superStreamPartitions = config.getInteger(SUPER_STREAM_PARTITIONS, 3);

        // Parse binding keys from list
        String bindingKeysStr = config.getString(SUPER_STREAM_BINDING_KEYS);
        if (bindingKeysStr != null) {
            superStreamBindingKeys = bindingKeysStr.split(",");
        }

        producerName = config.getString(PRODUCER_NAME);
        producerFilterValue = config.getString(PRODUCER_FILTER_VALUE);
        producerBatchSize = config.getInteger(PRODUCER_BATCH_SIZE, 100);
        producerSubEntrySize = config.getInteger(PRODUCER_SUB_ENTRY_SIZE, 1);
        producerMaxUnconfirmedMessages = config.getInteger(PRODUCER_MAX_UNCONFIRMED_MESSAGES, 10000);
        producerBatchPublishingDelay = config.getInteger(PRODUCER_BATCH_PUBLISHING_DELAY, 100);
        producerConfirmTimeout = config.getInteger(PRODUCER_CONFIRM_TIMEOUT, 30);
        producerEnqueueTimeout = config.getInteger(PRODUCER_ENQUEUE_TIMEOUT, 10);
        batchConfirmTimeout = config.getInteger(BATCH_CONFIRM_TIMEOUT, 30);
        nullValue = config.getString(NULL_VALUE);

        validateTlsOptions();
    }

    private void validateTlsOptions() {
        validateStoreType(keyStoreType, KEY_STORE_TYPE.name());
        if (STORE_TYPE_PEM.equalsIgnoreCase(keyStoreType)) {
            validatePair(keyStoreCertificateFilePath, KEY_STORE_CERTIFICATE_FILE_PATH.name(),
                    keyStoreKeyFilePath, KEY_STORE_KEY_FILE_PATH.name());
        }
        else if (keyStoreType != null && keyStoreFilePath == null) {
            throw new io.debezium.DebeziumException(KEY_STORE_FILE_PATH.name() + " is required when " + KEY_STORE_TYPE.name() + " is " + keyStoreType);
        }

        validateStoreType(trustStoreType, TRUST_STORE_TYPE.name());
        if (trustStoreType != null && trustStoreFilePath == null) {
            throw new io.debezium.DebeziumException(TRUST_STORE_FILE_PATH.name() + " is required when " + TRUST_STORE_TYPE.name() + " is set");
        }
    }

    private static void validateStoreType(String type, String fieldName) {
        if (type != null
                && !STORE_TYPE_PEM.equalsIgnoreCase(type)
                && !STORE_TYPE_PKCS12.equalsIgnoreCase(type)
                && !STORE_TYPE_JKS.equalsIgnoreCase(type)) {
            throw new io.debezium.DebeziumException("Unsupported " + fieldName + " '" + type + "'. Must be one of: PEM, PKCS12, JKS.");
        }
    }

    private static void validatePair(String first, String firstName, String second, String secondName) {
        if ((first == null) != (second == null)) {
            throw new io.debezium.DebeziumException(firstName + " and " + secondName + " must be configured together");
        }
    }

    public String getConnectionHost() {
        return connectionHost;
    }

    public Integer getConnectionPort() {
        return connectionPort;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public boolean isTlsEnable() {
        return tlsEnable;
    }

    public String getKeyStoreType() {
        return keyStoreType;
    }

    public String getKeyStoreCertificateFilePath() {
        return keyStoreCertificateFilePath;
    }

    public String getKeyStoreKeyFilePath() {
        return keyStoreKeyFilePath;
    }

    public String getKeyStoreFilePath() {
        return keyStoreFilePath;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public String getTrustStoreType() {
        return trustStoreType;
    }

    public String getTrustStoreFilePath() {
        return trustStoreFilePath;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public boolean isTlsVerifyHostname() {
        return tlsVerifyHostname;
    }

    public int getRpcTimeout() {
        return rpcTimeout;
    }

    public int getMaxProducersByConnection() {
        return maxProducersByConnection;
    }

    public int getMaxTrackingConsumersByConnection() {
        return maxTrackingConsumersByConnection;
    }

    public int getMaxConsumersByConnection() {
        return maxConsumersByConnection;
    }

    public int getRequestedHeartbeat() {
        return requestedHeartbeat;
    }

    public int getRequestedMaxFrameSize() {
        return requestedMaxFrameSize;
    }

    public String getId() {
        return id;
    }

    public String getStream() {
        return stream;
    }

    public Duration getStreamMaxAge() {
        return streamMaxAge;
    }

    public String getStreamMaxLength() {
        return streamMaxLength;
    }

    public String getStreamMaxSegmentSize() {
        return streamMaxSegmentSize;
    }

    public boolean isSuperStreamEnable() {
        return superStreamEnable;
    }

    public int getSuperStreamPartitions() {
        return superStreamPartitions;
    }

    public String[] getSuperStreamBindingKeys() {
        return superStreamBindingKeys;
    }

    public String getProducerName() {
        return producerName;
    }

    public String getProducerFilterValue() {
        return producerFilterValue;
    }

    public int getProducerBatchSize() {
        return producerBatchSize;
    }

    public int getProducerSubEntrySize() {
        return producerSubEntrySize;
    }

    public int getProducerMaxUnconfirmedMessages() {
        return producerMaxUnconfirmedMessages;
    }

    public int getProducerBatchPublishingDelay() {
        return producerBatchPublishingDelay;
    }

    public int getProducerConfirmTimeout() {
        return producerConfirmTimeout;
    }

    public int getProducerEnqueueTimeout() {
        return producerEnqueueTimeout;
    }

    public int getBatchConfirmTimeout() {
        return batchConfirmTimeout;
    }

    public String getNullValue() {
        return nullValue;
    }
}
