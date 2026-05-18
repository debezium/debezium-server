/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ydb;

import java.net.InetAddress;
import java.util.UUID;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;

import tech.ydb.topic.description.Codec;

/**
 * Configuration fields for {@link YdbChangeConsumer}.
 */
public class YdbChangeConsumerConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(YdbChangeConsumerConfig.class);

    public static final Field ENDPOINT = Field.create("endpoint")
            .withDisplayName("YDB endpoint")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("YDB endpoint, e.g. grpcs://ydb.example:2135.")
            .required();

    public static final Field DATABASE = Field.create("database")
            .withDisplayName("YDB database path")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("YDB database path, e.g. /Root/debezium.")
            .required();

    public static final Field AUTH_USER = Field.create("auth.user")
            .withDisplayName("YDB static auth login")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("YDB static credentials login. If unset, the sink connects anonymously.");

    public static final Field AUTH_PASSWORD = Field.create("auth.password")
            .withDisplayName("YDB static auth password")
            .withType(ConfigDef.Type.PASSWORD)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("YDB static credentials password.");

    public static final Field TOPIC = Field.create("topic")
            .withDisplayName("YDB topic path")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Full YDB topic path to write all change events to, e.g. /Root/debezium/cdc. "
                    + "If not set, the destination produced by streamNameMapper is used as the topic path "
                    + "(after applying topic.prefix).")
            .withDefault((String) null);

    public static final Field TOPIC_PREFIX = Field.create("topic.prefix")
            .withDisplayName("YDB topic path prefix")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Prefix prepended to the mapped destination when 'topic' is not set. "
                    + "Useful to put per-table topics under one database directory, e.g. '/Root/debezium/'.")
            .withDefault("");

    public static final Field PRODUCER_CODEC = Field.create("producer.codec")
            .withDisplayName("YDB topic codec")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Codec used by YDB topic writer: RAW, GZIP, ZSTD.")
            .withDefault("GZIP");

    public static final Field INSTANCE_ID = Field.create("instance.id")
            .withDisplayName("YDB writer instance id")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Unique id of this debezium-server process for YDB topic writes. It is passed to "
                    + "AsyncWriter as producer_id (together with the message group) and selects the partition "
                    + "writer session in YDB, so every replica that writes the same topic must use a distinct "
                    + "instance.id. When unset, defaults to '<connector.name>::<hostname>'.")
            .withDefault((String) null);

    public static final Field CONNECTOR_NAME = Field.create("connector.name")
            .withDisplayName("Logical connector name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Logical Debezium connector name. Used to build the default instance.id "
                    + "('<connector.name>::<hostname>') when instance.id is not set.")
            .withDefault("debezium-source");

    public static final Field WRITER_SHUTDOWN_TIMEOUT_MS = Field.create("writer.shutdown-timeout-ms")
            .withDisplayName("Writer shutdown timeout (ms)")
            .withType(ConfigDef.Type.LONG)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("How long to wait for AsyncWriter.shutdown() per writer at close time.")
            .withDefault(30_000L);

    public static final Field WRITER_ACK_TIMEOUT_MS = Field.create("writer.ack-timeout-ms")
            .withDisplayName("Writer write ack timeout (ms)")
            .withType(ConfigDef.Type.LONG)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("How long to wait for all AsyncWriter.send() acknowledgements in a batch.")
            .withDefault(30_000L);

    public static final Field TOPIC_AUTO_CREATE = Field.create("topic.auto.create")
            .withDisplayName("Auto-create YDB topics")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("When true, the sink creates each resolved YDB topic path on first use if it does not "
                    + "exist (lazy, once per path). Parent database directories must already exist. "
                    + "Optional topic.auto.create.initial.consumer registers a consumer for readers.")
            .withDefault(false);

    public static final Field TOPIC_AUTO_CREATE_MIN_ACTIVE_PARTITIONS = Field.create("topic.auto.create.min-active-partitions")
            .withDisplayName("Auto-create topic min active partitions")
            .withType(ConfigDef.Type.LONG)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Min (and max) active partitions for auto-created topics. Must be positive.")
            .withDefault(1L)
            .withValidation(Field::isPositiveLong);

    public static final Field TOPIC_AUTO_CREATE_INITIAL_CONSUMER = Field.create("topic.auto.create.initial.consumer")
            .withDisplayName("Auto-create initial consumer name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("When set, auto-created topics include this consumer (for readers). Leave unset for "
                    + "write-only topics.")
            .withDefault((String) null);

    public static final Field.Set ALL_FIELDS = Field.setOf(
            ENDPOINT,
            DATABASE,
            AUTH_USER,
            AUTH_PASSWORD,
            TOPIC,
            TOPIC_PREFIX,
            PRODUCER_CODEC,
            INSTANCE_ID,
            CONNECTOR_NAME,
            WRITER_SHUTDOWN_TIMEOUT_MS,
            WRITER_ACK_TIMEOUT_MS,
            TOPIC_AUTO_CREATE,
            TOPIC_AUTO_CREATE_MIN_ACTIVE_PARTITIONS,
            TOPIC_AUTO_CREATE_INITIAL_CONSUMER);

    private final String endpoint;
    private final String database;
    private final String authUser;
    private final String authPassword;
    private final String topic;
    private final String topicPrefix;
    private final int producerCodec;
    private final String resolvedInstanceId;
    private final String connectorName;
    private final long writerShutdownTimeoutMs;
    private final long writerAckTimeoutMs;
    private final boolean topicAutoCreate;
    private final long topicAutoCreateMinActivePartitions;
    private final String topicAutoCreateInitialConsumer;

    public YdbChangeConsumerConfig(Configuration config) {
        this.endpoint = config.getString(ENDPOINT);
        this.database = config.getString(DATABASE);
        this.authUser = config.getString(AUTH_USER);
        this.authPassword = config.getString(AUTH_PASSWORD);
        this.topic = config.getString(TOPIC);
        this.topicPrefix = config.getString(TOPIC_PREFIX);
        this.connectorName = config.getString(CONNECTOR_NAME);
        this.producerCodec = parseProducerCodec(config.getString(PRODUCER_CODEC));
        this.resolvedInstanceId = resolveInstanceId(config.getString(INSTANCE_ID), connectorName);
        this.writerShutdownTimeoutMs = config.getLong(WRITER_SHUTDOWN_TIMEOUT_MS);
        this.writerAckTimeoutMs = config.getLong(WRITER_ACK_TIMEOUT_MS);
        this.topicAutoCreate = config.getBoolean(TOPIC_AUTO_CREATE);
        this.topicAutoCreateMinActivePartitions = config.getLong(TOPIC_AUTO_CREATE_MIN_ACTIVE_PARTITIONS);
        this.topicAutoCreateInitialConsumer = config.getString(TOPIC_AUTO_CREATE_INITIAL_CONSUMER);
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getDatabase() {
        return database;
    }

    public String getAuthUser() {
        return authUser;
    }

    public String getAuthPassword() {
        return authPassword;
    }

    public String getTopic() {
        return topic;
    }

    public String getTopicPrefix() {
        return topicPrefix;
    }

    public int getProducerCodec() {
        return producerCodec;
    }

    public String getResolvedInstanceId() {
        return resolvedInstanceId;
    }

    public String getConnectorName() {
        return connectorName;
    }

    static int parseProducerCodec(String value) {
        if (value == null || value.isBlank()) {
            return Codec.GZIP;
        }
        return switch (value.trim().toUpperCase()) {
            case "RAW" -> Codec.RAW;
            case "GZIP" -> Codec.GZIP;
            case "ZSTD" -> Codec.ZSTD;
            default -> throw new DebeziumException("Unsupported YDB topic codec: " + value);
        };
    }

    static String resolveInstanceId(String configuredInstanceId, String connectorName) {
        if (configuredInstanceId != null && !configuredInstanceId.isBlank()) {
            return configuredInstanceId.trim();
        }
        return connectorName + "::" + localHostname();
    }

    static String resolveInstanceId(String configuredInstanceId, String connectorName, String hostname) {
        if (configuredInstanceId != null && !configuredInstanceId.isBlank()) {
            return configuredInstanceId.trim();
        }
        return connectorName + "::" + hostname;
    }

    private static String localHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        }
        catch (Exception e) {
            String fallback = UUID.randomUUID().toString();
            LOGGER.warn("Hostname lookup failed for default YDB instance.id; falling back to random UUID {}", fallback, e);
            return fallback;
        }
    }

    public long getWriterShutdownTimeoutMs() {
        return writerShutdownTimeoutMs;
    }

    public long getWriterAckTimeoutMs() {
        return writerAckTimeoutMs;
    }

    public boolean isTopicAutoCreate() {
        return topicAutoCreate;
    }

    public long getTopicAutoCreateMinActivePartitions() {
        return topicAutoCreateMinActivePartitions;
    }

    public String getTopicAutoCreateInitialConsumer() {
        return topicAutoCreateInitialConsumer;
    }
}
