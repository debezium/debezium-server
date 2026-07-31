/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link ZerobusChangeConsumer} (the gRPC route).
 * <p>
 * All properties are prefixed with {@code debezium.sink.zerobus.} in {@code application.properties}.
 */
public class ZerobusChangeConsumerConfig {

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

    public static final Field MAX_INFLIGHT_RECORDS = Field.create("max.inflight.records")
            .withDisplayName("Max in-flight records")
            .withType(ConfigDef.Type.INT)
            .withDefault(50000)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum number of un-acknowledged records per stream (non-blocking ingestion).");

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
    private final int maxInflightRecords;
    private final int metricsLogInterval;

    public ZerobusChangeConsumerConfig(Configuration config) {
        this.endpoint = config.getString(ENDPOINT);
        this.workspaceUrl = config.getString(WORKSPACE_URL);
        this.clientId = config.getString(CLIENT_ID);
        this.clientSecret = config.getString(CLIENT_SECRET);
        this.table = config.getString(TABLE);
        this.maxInflightRecords = config.getInteger(MAX_INFLIGHT_RECORDS);
        this.metricsLogInterval = config.getInteger(METRICS_LOG_INTERVAL);
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

    public int getMaxInflightRecords() {
        return maxInflightRecords;
    }

    public int getMetricsLogInterval() {
        return metricsLogInterval;
    }
}
