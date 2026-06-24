/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link ZeroBusChangeConsumer}.
 */
public class ZeroBusSinkConfig {

    public static final int DEFAULT_BATCH_SIZE = 200;
    public static final int DEFAULT_RETRIES = 5;
    public static final int DEFAULT_MAX_INFLIGHT_RECORDS = 1_000_000;
    public static final int DEFAULT_MAX_INFLIGHT_BATCHES = 1_000;

    public static final String AUTHENTICATION_OAUTH2 = "oauth2";
    public static final String RECORD_FORMAT_JSON = "json";
    public static final String RECORD_FORMAT_PROTOBUF = "protobuf";
    public static final String RECORD_FORMAT_ARROW = "arrow";
    public static final String TABLE_MAPPING_SOURCE = "source";
    public static final String TABLE_MAPPING_EXPLICIT = "explicit";
    public static final String TABLE_MAPPING_REGEX_MODE = "regex";
    public static final String IDEMPOTENCY_SOURCE = "source";
    public static final String IDEMPOTENCY_NONE = "none";
    public static final String TOMBSTONE_HANDLING_EVENT = "event";
    public static final String TOMBSTONE_HANDLING_DROP = "drop";

    public static final Field ENDPOINT = Field.create("endpoint")
            .withDisplayName("ZeroBus endpoint")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("ZeroBus native ingest endpoint.");

    public static final Field WORKSPACE_URL = Field.create("workspace.url")
            .withDisplayName("Databricks workspace URL")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Databricks workspace URL used as the Unity Catalog endpoint by the ZeroBus SDK.");

    public static final Field AUTHENTICATION_TYPE = Field.create("authentication.type")
            .withDisplayName("Authentication type")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("ZeroBus authentication type. The initial supported value is 'oauth2'.");

    public static final Field OAUTH2_CLIENT_ID = Field.create("authentication.oauth2.client-id")
            .withDisplayName("OAuth2 client ID")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("OAuth2 client ID used by the ZeroBus SDK-backed client.");

    public static final Field OAUTH2_CLIENT_SECRET = Field.create("authentication.oauth2.client-secret")
            .withDisplayName("OAuth2 client secret")
            .withType(ConfigDef.Type.PASSWORD)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("OAuth2 client secret used by the ZeroBus SDK-backed client.");

    public static final Field RECORD_FORMAT = Field.create("record.format")
            .withDisplayName("ZeroBus record format")
            .withType(ConfigDef.Type.STRING)
            .withDefault(RECORD_FORMAT_JSON)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("ZeroBus SDK record format. Supported values are 'json', 'protobuf', and 'arrow'.");

    public static final Field TABLE_MAPPING_MODE = Field.create("table.mapping.mode")
            .withDisplayName("Table mapping mode")
            .withType(ConfigDef.Type.STRING)
            .withDefault(TABLE_MAPPING_SOURCE)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Table mapping mode: 'source', 'explicit', or 'regex'.");

    public static final Field TABLE_MAPPING_DEFAULT_CATALOG = Field.create("table.mapping.default.catalog")
            .withDisplayName("Default Unity Catalog catalog")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Default Unity Catalog catalog used by source and regex table mapping modes.");

    public static final Field TABLE_MAPPING_DEFAULT_SCHEMA = Field.create("table.mapping.default.schema")
            .withDisplayName("Default Unity Catalog schema")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Default Unity Catalog schema used by source table mapping mode.");

    public static final Field TABLE_MAPPING_OVERRIDES = Field.create("table.mapping.overrides")
            .withDisplayName("Explicit table mapping overrides")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Comma-separated mappings in the form '<source-destination>=<catalog.schema.table>'.");

    public static final Field TABLE_MAPPING_REGEX = Field.create("table.mapping.regex")
            .withDisplayName("Regex table mapping pattern")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Regex applied to the Debezium event destination when table.mapping.mode=regex.");

    public static final Field TABLE_MAPPING_REPLACEMENT = Field.create("table.mapping.replacement")
            .withDisplayName("Regex table mapping replacement")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Replacement that must produce a Unity Catalog table identifier.");

    public static final Field BATCH_SIZE = Field.create("batch.size")
            .withDisplayName("Batch size")
            .withType(ConfigDef.Type.INT)
            .withDefault(DEFAULT_BATCH_SIZE)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum number of records to write in one ZeroBus request.");

    public static final Field RETRIES = Field.create("retries")
            .withDisplayName("Max retries")
            .withType(ConfigDef.Type.INT)
            .withDefault(DEFAULT_RETRIES)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum retry attempts for retryable ZeroBus writes.");

    public static final Field RETRY_INTERVAL_MS = Field.create("retry.interval.ms")
            .withDisplayName("Retry interval (ms)")
            .withType(ConfigDef.Type.LONG)
            .withDefault(1000L)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Interval in milliseconds between retryable ZeroBus write attempts.");

    public static final Field TIMEOUT_MS = Field.create("timeout.ms")
            .withDisplayName("Client timeout (ms)")
            .withType(ConfigDef.Type.LONG)
            .withDefault(60000L)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Timeout in milliseconds for the SDK-backed ZeroBus client.");

    public static final Field MAX_INFLIGHT_RECORDS = Field.create("max.inflight.records")
            .withDisplayName("Maximum in-flight records")
            .withType(ConfigDef.Type.INT)
            .withDefault(DEFAULT_MAX_INFLIGHT_RECORDS)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum unacknowledged records allowed by the ZeroBus SDK stream.");

    public static final Field MAX_INFLIGHT_BATCHES = Field.create("max.inflight.batches")
            .withDisplayName("Maximum in-flight Arrow batches")
            .withType(ConfigDef.Type.INT)
            .withDefault(DEFAULT_MAX_INFLIGHT_BATCHES)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum unacknowledged Arrow batches allowed by the ZeroBus SDK Arrow stream.");

    public static final Field IDEMPOTENCY_MODE = Field.create("idempotency.mode")
            .withDisplayName("Idempotency mode")
            .withType(ConfigDef.Type.STRING)
            .withDefault(IDEMPOTENCY_SOURCE)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Idempotency mode: 'source' writes a deterministic envelope-level idempotency_key from destination, partition, key, and SourceRecord source partition/offset when available; 'none' omits it.");

    public static final Field TOMBSTONE_HANDLING_MODE = Field.create("tombstone.handling.mode")
            .withDisplayName("Tombstone handling mode")
            .withType(ConfigDef.Type.STRING)
            .withDefault(TOMBSTONE_HANDLING_EVENT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Null-value tombstone handling mode: 'event' writes an operation=tombstone row, 'drop' skips tombstones.");

    private final String endpoint;
    private final String workspaceUrl;
    private final String authenticationType;
    private final String oauth2ClientId;
    private final String oauth2ClientSecret;
    private final String recordFormat;
    private final String tableMappingMode;
    private final String tableMappingDefaultCatalog;
    private final String tableMappingDefaultSchema;
    private final Map<String, String> tableMappingOverrides;
    private final String tableMappingRegex;
    private final String tableMappingReplacement;
    private final int batchSize;
    private final int retries;
    private final Duration retryInterval;
    private final Duration timeout;
    private final int maxInflightRecords;
    private final int maxInflightBatches;
    private final String idempotencyMode;
    private final String tombstoneHandlingMode;

    public ZeroBusSinkConfig(Configuration config) {
        endpoint = trimToNull(config.getString(ENDPOINT));
        workspaceUrl = trimToNull(config.getString(WORKSPACE_URL));
        authenticationType = lower(config.getString(AUTHENTICATION_TYPE));
        oauth2ClientId = trimToNull(config.getString(OAUTH2_CLIENT_ID));
        oauth2ClientSecret = trimToNull(config.getString(OAUTH2_CLIENT_SECRET));
        recordFormat = lower(config.getString(RECORD_FORMAT));
        tableMappingMode = lower(config.getString(TABLE_MAPPING_MODE));
        tableMappingDefaultCatalog = trimToNull(config.getString(TABLE_MAPPING_DEFAULT_CATALOG));
        tableMappingDefaultSchema = trimToNull(config.getString(TABLE_MAPPING_DEFAULT_SCHEMA));
        tableMappingOverrides = parseOverrides(config.getString(TABLE_MAPPING_OVERRIDES));
        tableMappingRegex = trimToNull(config.getString(TABLE_MAPPING_REGEX));
        tableMappingReplacement = trimToNull(config.getString(TABLE_MAPPING_REPLACEMENT));
        batchSize = config.getInteger(BATCH_SIZE);
        retries = config.getInteger(RETRIES);
        retryInterval = Duration.ofMillis(config.getLong(RETRY_INTERVAL_MS));
        timeout = Duration.ofMillis(config.getLong(TIMEOUT_MS));
        maxInflightRecords = config.getInteger(MAX_INFLIGHT_RECORDS);
        maxInflightBatches = config.getInteger(MAX_INFLIGHT_BATCHES);
        idempotencyMode = lower(config.getString(IDEMPOTENCY_MODE));
        tombstoneHandlingMode = lower(config.getString(TOMBSTONE_HANDLING_MODE));
    }

    public void validate() {
        require(endpoint, ENDPOINT.name());
        require(workspaceUrl, WORKSPACE_URL.name());
        require(authenticationType, AUTHENTICATION_TYPE.name());

        if (!AUTHENTICATION_OAUTH2.equals(authenticationType)) {
            throw new DebeziumException("Unsupported ZeroBus authentication type '" + authenticationType + "'");
        }
        require(oauth2ClientId, OAUTH2_CLIENT_ID.name());
        require(oauth2ClientSecret, OAUTH2_CLIENT_SECRET.name());
        if (!RECORD_FORMAT_JSON.equals(recordFormat) && !RECORD_FORMAT_PROTOBUF.equals(recordFormat) && !RECORD_FORMAT_ARROW.equals(recordFormat)) {
            throw new DebeziumException("Unsupported ZeroBus record format '" + recordFormat + "'");
        }

        if (TABLE_MAPPING_SOURCE.equals(tableMappingMode)) {
            require(tableMappingDefaultCatalog, TABLE_MAPPING_DEFAULT_CATALOG.name());
            require(tableMappingDefaultSchema, TABLE_MAPPING_DEFAULT_SCHEMA.name());
        }
        else if (TABLE_MAPPING_EXPLICIT.equals(tableMappingMode)) {
            if (tableMappingOverrides.isEmpty()) {
                throw new DebeziumException("ZeroBus table.mapping.overrides is required when table.mapping.mode=explicit");
            }
            tableMappingOverrides.values().forEach(ZeroBusTableRouter::validateUnityCatalogTable);
        }
        else if (TABLE_MAPPING_REGEX_MODE.equals(tableMappingMode)) {
            require(tableMappingRegex, TABLE_MAPPING_REGEX.name());
            require(tableMappingReplacement, TABLE_MAPPING_REPLACEMENT.name());
            try {
                Pattern.compile(tableMappingRegex);
            }
            catch (PatternSyntaxException e) {
                throw new DebeziumException("Invalid ZeroBus table mapping regex '" + tableMappingRegex + "'", e);
            }
        }
        else {
            throw new DebeziumException("Unsupported ZeroBus table mapping mode '" + tableMappingMode + "'");
        }

        if (batchSize <= 0) {
            throw new DebeziumException("ZeroBus batch.size must be greater than 0");
        }
        if (retries <= 0) {
            throw new DebeziumException("ZeroBus retries must be greater than 0");
        }
        if (retryInterval.isNegative()) {
            throw new DebeziumException("ZeroBus retry.interval.ms must not be negative");
        }
        if (timeout.isNegative() || timeout.isZero()) {
            throw new DebeziumException("ZeroBus timeout.ms must be greater than 0");
        }
        if (maxInflightRecords <= 0) {
            throw new DebeziumException("ZeroBus max.inflight.records must be greater than 0");
        }
        if (maxInflightBatches <= 0) {
            throw new DebeziumException("ZeroBus max.inflight.batches must be greater than 0");
        }
        if (!IDEMPOTENCY_SOURCE.equals(idempotencyMode) && !IDEMPOTENCY_NONE.equals(idempotencyMode)) {
            throw new DebeziumException("Unsupported ZeroBus idempotency mode '" + idempotencyMode + "'");
        }
        if (!TOMBSTONE_HANDLING_EVENT.equals(tombstoneHandlingMode) && !TOMBSTONE_HANDLING_DROP.equals(tombstoneHandlingMode)) {
            throw new DebeziumException("Unsupported ZeroBus tombstone handling mode '" + tombstoneHandlingMode + "'");
        }
    }

    private static Map<String, String> parseOverrides(String raw) {
        Map<String, String> overrides = new LinkedHashMap<>();
        String value = trimToNull(raw);
        if (value == null) {
            return overrides;
        }

        for (String mapping : value.split(",")) {
            String entry = mapping.trim();
            if (entry.isEmpty()) {
                continue;
            }
            int separator = entry.indexOf('=');
            if (separator <= 0 || separator == entry.length() - 1) {
                throw new DebeziumException("Invalid ZeroBus table mapping override '" + entry + "'");
            }
            overrides.put(entry.substring(0, separator).trim(), entry.substring(separator + 1).trim());
        }
        return Map.copyOf(overrides);
    }

    private static void require(String value, String fieldName) {
        if (value == null) {
            throw new DebeziumException("Missing required ZeroBus configuration property '" + fieldName + "'");
        }
    }

    private static String lower(String value) {
        String trimmed = trimToNull(value);
        return trimmed == null ? null : trimmed.toLowerCase(Locale.ROOT);
    }

    private static String trimToNull(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return value.trim();
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getWorkspaceUrl() {
        return workspaceUrl;
    }

    public String getAuthenticationType() {
        return authenticationType;
    }

    public String getOauth2ClientId() {
        return oauth2ClientId;
    }

    public String getOauth2ClientSecret() {
        return oauth2ClientSecret;
    }

    public String getRecordFormat() {
        return recordFormat;
    }

    public String getTableMappingMode() {
        return tableMappingMode;
    }

    public String getTableMappingDefaultCatalog() {
        return tableMappingDefaultCatalog;
    }

    public String getTableMappingDefaultSchema() {
        return tableMappingDefaultSchema;
    }

    public Map<String, String> getTableMappingOverrides() {
        return tableMappingOverrides;
    }

    public String getTableMappingRegex() {
        return tableMappingRegex;
    }

    public String getTableMappingReplacement() {
        return tableMappingReplacement;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getRetries() {
        return retries;
    }

    public Duration getRetryInterval() {
        return retryInterval;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public int getMaxInflightRecords() {
        return maxInflightRecords;
    }

    public int getMaxInflightBatches() {
        return maxInflightBatches;
    }

    public String getIdempotencyMode() {
        return idempotencyMode;
    }

    public String getTombstoneHandlingMode() {
        return tombstoneHandlingMode;
    }
}
