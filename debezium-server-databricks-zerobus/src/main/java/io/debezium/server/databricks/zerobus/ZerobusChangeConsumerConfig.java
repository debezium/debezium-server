/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link ZerobusChangeConsumer} (the gRPC route).
 * <p>
 * All properties are prefixed with {@code debezium.sink.zerobus.} in {@code application.properties}.
 */
public class ZerobusChangeConsumerConfig {

    /**
     * The shape of the row the sink writes, which is the data contract of the target table. The two
     * modes are mutually exclusive: a table is created for one of them, and pointing the other at that
     * table would not match its columns.
     */
    public enum PayloadMode implements EnumeratedValue {

        /**
         * Each change event becomes a typed Delta row: {@link ZerobusTypeSystem} maps every value onto
         * the shape the target column expects, so the table is queried with its own column types.
         */
        TYPED("typed"),

        /**
         * Each change event is landed as a raw CDC envelope, preserving the event so that typed
         * materialization can happen downstream rather than in the sink.
         */
        ENVELOPE("envelope");

        private final String value;

        PayloadMode(String value) {
            this.value = value;
        }

        public static PayloadMode parse(String value) {
            for (PayloadMode option : PayloadMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return PayloadMode.TYPED;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    /**
     * The encoding of a record on the wire. Zerobus binds an encoding to a stream, so this selects
     * which kind of stream the sink opens for a table.
     */
    public enum RecordFormat implements EnumeratedValue {

        JSON("json"),

        PROTOBUF("protobuf");

        private final String value;

        RecordFormat(String value) {
            this.value = value;
        }

        public static RecordFormat parse(String value) {
            for (RecordFormat option : RecordFormat.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return RecordFormat.JSON;
        }

        @Override
        public String getValue() {
            return value;
        }
    }

    public static final Field ENDPOINT = Field.create("endpoint")
            .withDisplayName("Zerobus gRPC endpoint")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("Zerobus gRPC endpoint, e.g. <workspace-id>.zerobus.<region>.cloud.databricks.com");

    public static final Field WORKSPACE_URL = Field.create("workspace.url")
            .withDisplayName("Databricks workspace URL")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("Databricks workspace URL, e.g. https://dbc-xxxx.cloud.databricks.com. "
                    + "Passed to the Zerobus SDK as the Unity Catalog endpoint (used by the native client for "
                    + "authentication and table resolution), so it is required for the gRPC route.");

    public static final Field CLIENT_ID = Field.create("client.id")
            .withDisplayName("Service principal client id")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("OAuth client id (service principal application id).");

    public static final Field CLIENT_SECRET = Field.create("client.secret")
            .withDisplayName("Service principal client secret")
            .withType(ConfigDef.Type.PASSWORD)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("OAuth client secret for the service principal.");

    public static final Field TABLE = Field.create("table")
            .withDisplayName("Default target table")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Fully qualified default target table (catalog.schema.table). Used when the "
                    + "mapped topic name is not already a fully qualified table name.");

    public static final Field PAYLOAD_MODE = Field.create("payload.mode")
            .withDisplayName("Payload mode")
            .withEnum(PayloadMode.class, PayloadMode.TYPED)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The shape of the row written to the target table. "
                    + "'typed' (the default) maps each change event onto the target table's own column types. "
                    + "'envelope' lands the change event as a raw CDC envelope, leaving typed materialization downstream. "
                    + "The two are different data contracts for a table, so a table is created for one of them.");

    public static final Field RECORD_FORMAT = Field.create("record.format")
            .withDisplayName("Record format")
            .withEnum(RecordFormat.class, RecordFormat.JSON)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("The encoding of a record on the wire. 'json' (the default) is supported by both "
                    + "payload modes; 'protobuf' is supported by the 'envelope' payload mode.");

    public static final Field MAX_INFLIGHT_RECORDS = Field.create("max.inflight.records")
            .withDisplayName("Max in-flight records")
            .withType(ConfigDef.Type.INT)
            .withDefault(50000)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum number of un-acknowledged records per stream (non-blocking ingestion).");

    public static final Field MAX_OPEN_STREAMS = Field.create("max.open.streams")
            .withDisplayName("Max open streams")
            .withType(ConfigDef.Type.INT)
            .withDefault(100)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum number of Zerobus streams kept open at once. Zerobus binds one stream to one "
                    + "table, so a source with many tables would otherwise hold an unbounded number of connections. "
                    + "When the limit is exceeded, the least recently used streams are flushed and closed, and are "
                    + "reopened on demand. Set to 0 to keep every stream open.");

    // The recovery options below deliberately declare no default: the value is passed to the SDK
    // only when it is set, so that an unset option keeps whatever the SDK's own default is rather
    // than having this sink pin it to a value that could drift from the SDK across upgrades.

    public static final Field RECOVERY = Field.create("recovery")
            .withDisplayName("Enable stream recovery")
            .withType(ConfigDef.Type.BOOLEAN)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Whether the Zerobus SDK recovers a stream that fails with a retriable error, "
                    + "re-sending the records it has not acknowledged. When this option is not set, the SDK default applies.");

    public static final Field RECOVERY_RETRIES = Field.create("recovery.retries")
            .withDisplayName("Stream recovery retries")
            .withType(ConfigDef.Type.INT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum number of times the SDK attempts to recover a failed stream. "
                    + "When this option is not set, the SDK default applies.");

    public static final Field RECOVERY_BACKOFF_MS = Field.create("recovery.backoff.ms")
            .withDisplayName("Stream recovery backoff")
            .withType(ConfigDef.Type.INT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Delay, in milliseconds, between stream recovery attempts. "
                    + "When this option is not set, the SDK default applies.");

    public static final Field RECOVERY_TIMEOUT_MS = Field.create("recovery.timeout.ms")
            .withDisplayName("Stream recovery timeout")
            .withType(ConfigDef.Type.INT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Time, in milliseconds, after which a stream recovery attempt is abandoned. "
                    + "When this option is not set, the SDK default applies.");

    public static final Field FLUSH_TIMEOUT_MS = Field.create("flush.timeout.ms")
            .withDisplayName("Flush timeout")
            .withType(ConfigDef.Type.INT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Time, in milliseconds, that a flush waits for the records in the stream to be "
                    + "acknowledged. When this option is not set, the SDK default applies.");

    public static final Field METRICS_LOG_INTERVAL = Field.create("metrics.log.interval")
            .withDisplayName("Metrics log interval (batches)")
            .withType(ConfigDef.Type.INT)
            .withDefault(0)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Emit a periodic INFO log line summarizing the sink metrics every N processed batches. "
                    + "0 (the default) disables periodic logging; the metrics remain available over JMX regardless.");

    private final String endpoint;
    private final String workspaceUrl;
    private final String clientId;
    private final String clientSecret;
    private final String table;
    private final PayloadMode payloadMode;
    private final RecordFormat recordFormat;
    private final int maxInflightRecords;
    private final int maxOpenStreams;
    private final Boolean recovery;
    private final Integer recoveryRetries;
    private final Integer recoveryBackoffMs;
    private final Integer recoveryTimeoutMs;
    private final Integer flushTimeoutMs;
    private final int metricsLogInterval;

    public ZerobusChangeConsumerConfig(Configuration config) {
        this.endpoint = config.getString(ENDPOINT);
        this.workspaceUrl = config.getString(WORKSPACE_URL);
        this.clientId = config.getString(CLIENT_ID);
        this.clientSecret = config.getString(CLIENT_SECRET);
        this.table = config.getString(TABLE);
        this.payloadMode = PayloadMode.parse(config.getString(PAYLOAD_MODE));
        this.recordFormat = RecordFormat.parse(config.getString(RECORD_FORMAT));
        // Rejected here rather than at the first record: a combination the sink cannot honour would
        // otherwise open a stream and only fail once events start flowing.
        if (payloadMode == PayloadMode.TYPED && recordFormat == RecordFormat.PROTOBUF) {
            throw new io.debezium.DebeziumException(
                    "Unsupported combination of '" + PAYLOAD_MODE.name() + "=" + payloadMode.getValue()
                            + "' and '" + RECORD_FORMAT.name() + "=" + recordFormat.getValue()
                            + "'. The typed payload mode writes JSON; use '" + RECORD_FORMAT.name() + "="
                            + RecordFormat.JSON.getValue() + "', or select the '" + PayloadMode.ENVELOPE.getValue()
                            + "' payload mode.");
        }
        this.maxInflightRecords = config.getInteger(MAX_INFLIGHT_RECORDS);
        this.maxOpenStreams = config.getInteger(MAX_OPEN_STREAMS);
        // Read through hasKey, because Configuration.getInteger/getBoolean parse the raw value and
        // throw on an absent one rather than returning null. Null here is meaningful: it tells the
        // stream builder to leave the corresponding SDK default alone.
        this.recovery = optionalBoolean(config, RECOVERY);
        this.recoveryRetries = optionalInteger(config, RECOVERY_RETRIES);
        this.recoveryBackoffMs = optionalInteger(config, RECOVERY_BACKOFF_MS);
        this.recoveryTimeoutMs = optionalInteger(config, RECOVERY_TIMEOUT_MS);
        this.flushTimeoutMs = optionalInteger(config, FLUSH_TIMEOUT_MS);
        this.metricsLogInterval = config.getInteger(METRICS_LOG_INTERVAL);
    }

    private static Integer optionalInteger(Configuration config, Field field) {
        return config.hasKey(field.name()) ? config.getInteger(field) : null;
    }

    private static Boolean optionalBoolean(Configuration config, Field field) {
        return config.hasKey(field.name()) ? config.getBoolean(field) : null;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getWorkspaceUrl() {
        return workspaceUrl;
    }

    public String getClientId() {
        return clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public String getTable() {
        return table;
    }

    /** @return the shape of the row written to the target table */
    public PayloadMode getPayloadMode() {
        return payloadMode;
    }

    /** @return the encoding of a record on the wire */
    public RecordFormat getRecordFormat() {
        return recordFormat;
    }

    public int getMaxInflightRecords() {
        return maxInflightRecords;
    }

    public int getMaxOpenStreams() {
        return maxOpenStreams;
    }

    /** @return whether stream recovery is enabled, or {@code null} to keep the SDK default */
    public Boolean getRecovery() {
        return recovery;
    }

    /** @return the number of recovery attempts, or {@code null} to keep the SDK default */
    public Integer getRecoveryRetries() {
        return recoveryRetries;
    }

    /** @return the delay between recovery attempts, or {@code null} to keep the SDK default */
    public Integer getRecoveryBackoffMs() {
        return recoveryBackoffMs;
    }

    /** @return the recovery attempt timeout, or {@code null} to keep the SDK default */
    public Integer getRecoveryTimeoutMs() {
        return recoveryTimeoutMs;
    }

    /** @return the flush timeout, or {@code null} to keep the SDK default */
    public Integer getFlushTimeoutMs() {
        return flushTimeoutMs;
    }

    public int getMetricsLogInterval() {
        return metricsLogInterval;
    }
}
