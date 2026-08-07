/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

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

    static final int DEFAULT_MAX_RECORD_BYTES = 10_000_000;
    static final String JSON_FLEXIBLE_FIELDS_STRING = "string";
    static final String JSON_FLEXIBLE_FIELDS_OBJECT = "object";
    static final String IDEMPOTENCY_SOURCE = "source";
    static final String IDEMPOTENCY_NONE = "none";
    static final String TOMBSTONE_EVENT = "event";
    static final String TOMBSTONE_DROP = "drop";
    static final String FILTER_MALFORMED_FAIL = "fail";
    static final String FILTER_MALFORMED_DROP = "drop";

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

    public static final Field MAX_RECORD_BYTES = Field.create("max.record.bytes")
            .withDisplayName("Maximum encoded record size")
            .withType(ConfigDef.Type.INT)
            .withDefault(DEFAULT_MAX_RECORD_BYTES)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Maximum encoded JSON or Protobuf envelope size in bytes. Oversized records fail "
                    + "before ingestion so their source offsets are not committed.");

    public static final Field JSON_FLEXIBLE_FIELDS_ENCODING = Field.create("json.flexible.fields.encoding")
            .withDisplayName("JSON flexible-field encoding")
            .withType(ConfigDef.Type.STRING)
            .withDefault(JSON_FLEXIBLE_FIELDS_STRING)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Encoding for envelope key, value, source_position, and headers. 'string' writes "
                    + "canonical JSON strings suitable for VARIANT columns; 'object' writes nested JSON values.");

    public static final Field IDEMPOTENCY_MODE = Field.create("idempotency.mode")
            .withDisplayName("Envelope idempotency mode")
            .withType(ConfigDef.Type.STRING)
            .withDefault(IDEMPOTENCY_SOURCE)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("'source' writes a deterministic idempotency_key from the source partition and "
                    + "offset plus event identity; 'none' omits it.");

    public static final Field TOMBSTONE_HANDLING_MODE = Field.create("tombstone.handling.mode")
            .withDisplayName("Envelope tombstone handling")
            .withType(ConfigDef.Type.STRING)
            .withDefault(TOMBSTONE_EVENT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("'event' writes a distinct operation=tombstone envelope; 'drop' intentionally skips it.");

    public static final Field FILTER_DESTINATION_REGEX = Field.create("filter.destination.regex")
            .withDisplayName("Destination include regex")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Optional regular expression that a Debezium destination must match.");

    public static final Field FILTER_OPERATIONS = Field.create("filter.operations")
            .withDisplayName("Included operations")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Optional comma-separated operation allowlist: create, read, update, change, delete, tombstone.");

    public static final Field FILTER_HEADER_NAME = Field.create("filter.header.name")
            .withDisplayName("Header filter name")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Header name evaluated by filter.header.value.regex.");

    public static final Field FILTER_HEADER_VALUE_REGEX = Field.create("filter.header.value.regex")
            .withDisplayName("Header value include regex")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Optional regular expression matched against the configured header value.");

    public static final Field FILTER_VALUE_JSON_POINTER = Field.create("filter.value.json.pointer")
            .withDisplayName("Value filter JSON pointer")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("JSON Pointer selecting the serialized event value evaluated by filter.value.regex.");

    public static final Field FILTER_VALUE_REGEX = Field.create("filter.value.regex")
            .withDisplayName("Value include regex")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Optional regular expression matched against the selected JSON value.");

    public static final Field FILTER_MALFORMED_MODE = Field.create("filter.malformed.mode")
            .withDisplayName("Malformed value filter handling")
            .withType(ConfigDef.Type.STRING)
            .withDefault(FILTER_MALFORMED_FAIL)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("'fail' stops the batch when a value filter cannot be evaluated; 'drop' skips it.");

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
    private final int maxRecordBytes;
    private final String jsonFlexibleFieldsEncoding;
    private final String idempotencyMode;
    private final String tombstoneHandlingMode;
    private final String filterDestinationRegex;
    private final Set<ZerobusOperation> filterOperations;
    private final String filterHeaderName;
    private final String filterHeaderValueRegex;
    private final String filterValueJsonPointer;
    private final String filterValueRegex;
    private final String filterMalformedMode;
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
        this.maxRecordBytes = config.getInteger(MAX_RECORD_BYTES);
        this.jsonFlexibleFieldsEncoding = lower(config.getString(JSON_FLEXIBLE_FIELDS_ENCODING));
        this.idempotencyMode = lower(config.getString(IDEMPOTENCY_MODE));
        this.tombstoneHandlingMode = lower(config.getString(TOMBSTONE_HANDLING_MODE));
        this.filterDestinationRegex = trimToNull(config.getString(FILTER_DESTINATION_REGEX));
        this.filterOperations = parseOperations(config.getString(FILTER_OPERATIONS));
        this.filterHeaderName = trimToNull(config.getString(FILTER_HEADER_NAME));
        this.filterHeaderValueRegex = trimToNull(config.getString(FILTER_HEADER_VALUE_REGEX));
        this.filterValueJsonPointer = trimToNull(config.getString(FILTER_VALUE_JSON_POINTER));
        this.filterValueRegex = trimToNull(config.getString(FILTER_VALUE_REGEX));
        this.filterMalformedMode = lower(config.getString(FILTER_MALFORMED_MODE));
        validateEnvelopeOptions();
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

    private void validateEnvelopeOptions() {
        if (maxRecordBytes <= 0) {
            throw new io.debezium.DebeziumException("Zerobus max.record.bytes must be greater than 0");
        }
        if (!JSON_FLEXIBLE_FIELDS_STRING.equals(jsonFlexibleFieldsEncoding)
                && !JSON_FLEXIBLE_FIELDS_OBJECT.equals(jsonFlexibleFieldsEncoding)) {
            throw new io.debezium.DebeziumException(
                    "Unsupported Zerobus JSON flexible-field encoding '" + jsonFlexibleFieldsEncoding + "'");
        }
        if (!IDEMPOTENCY_SOURCE.equals(idempotencyMode) && !IDEMPOTENCY_NONE.equals(idempotencyMode)) {
            throw new io.debezium.DebeziumException("Unsupported Zerobus idempotency mode '" + idempotencyMode + "'");
        }
        if (!TOMBSTONE_EVENT.equals(tombstoneHandlingMode) && !TOMBSTONE_DROP.equals(tombstoneHandlingMode)) {
            throw new io.debezium.DebeziumException(
                    "Unsupported Zerobus tombstone handling mode '" + tombstoneHandlingMode + "'");
        }
        validateRegex(filterDestinationRegex, FILTER_DESTINATION_REGEX.name());
        validatePair(filterHeaderName, FILTER_HEADER_NAME.name(), filterHeaderValueRegex, FILTER_HEADER_VALUE_REGEX.name());
        validateRegex(filterHeaderValueRegex, FILTER_HEADER_VALUE_REGEX.name());
        validatePair(filterValueJsonPointer, FILTER_VALUE_JSON_POINTER.name(), filterValueRegex, FILTER_VALUE_REGEX.name());
        if (filterValueJsonPointer != null && !filterValueJsonPointer.startsWith("/")) {
            throw new io.debezium.DebeziumException("Zerobus filter.value.json.pointer must start with '/'");
        }
        validateRegex(filterValueRegex, FILTER_VALUE_REGEX.name());
        if (!FILTER_MALFORMED_FAIL.equals(filterMalformedMode) && !FILTER_MALFORMED_DROP.equals(filterMalformedMode)) {
            throw new io.debezium.DebeziumException(
                    "Unsupported Zerobus filter malformed mode '" + filterMalformedMode + "'");
        }
    }

    private static Set<ZerobusOperation> parseOperations(String raw) {
        String value = trimToNull(raw);
        if (value == null) {
            return Set.of();
        }
        Set<ZerobusOperation> operations = new HashSet<>();
        for (String token : value.split(",")) {
            String operation = trimToNull(token);
            if (operation == null) {
                continue;
            }
            try {
                operations.add(ZerobusOperation.valueOf(operation.toUpperCase(Locale.ROOT)));
            }
            catch (IllegalArgumentException e) {
                throw new io.debezium.DebeziumException("Unsupported Zerobus filter operation '" + operation + "'", e);
            }
        }
        return Set.copyOf(operations);
    }

    private static void validatePair(String first, String firstName, String second, String secondName) {
        if ((first == null) != (second == null)) {
            throw new io.debezium.DebeziumException(
                    "Zerobus " + firstName + " and " + secondName + " must be configured together");
        }
    }

    private static void validateRegex(String expression, String fieldName) {
        if (expression == null) {
            return;
        }
        try {
            Pattern.compile(expression);
        }
        catch (PatternSyntaxException e) {
            throw new io.debezium.DebeziumException("Invalid Zerobus " + fieldName + " regex '" + expression + "'", e);
        }
    }

    private static String lower(String value) {
        String trimmed = trimToNull(value);
        return trimmed == null ? null : trimmed.toLowerCase(Locale.ROOT);
    }

    private static String trimToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
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

    public int getMaxRecordBytes() {
        return maxRecordBytes;
    }

    public String getJsonFlexibleFieldsEncoding() {
        return jsonFlexibleFieldsEncoding;
    }

    public String getIdempotencyMode() {
        return idempotencyMode;
    }

    public String getTombstoneHandlingMode() {
        return tombstoneHandlingMode;
    }

    public String getFilterDestinationRegex() {
        return filterDestinationRegex;
    }

    public Set<ZerobusOperation> getFilterOperations() {
        return filterOperations;
    }

    public String getFilterHeaderName() {
        return filterHeaderName;
    }

    public String getFilterHeaderValueRegex() {
        return filterHeaderValueRegex;
    }

    public String getFilterValueJsonPointer() {
        return filterValueJsonPointer;
    }

    public String getFilterValueRegex() {
        return filterValueRegex;
    }

    public String getFilterMalformedMode() {
        return filterMalformedMode;
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
