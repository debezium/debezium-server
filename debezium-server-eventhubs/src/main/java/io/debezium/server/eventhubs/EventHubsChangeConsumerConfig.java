/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.eventhubs;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link EventHubsChangeConsumer}.
 */
public class EventHubsChangeConsumerConfig {

    public static final Field CONNECTION_STRING = Field.create("connectionstring")
            .withDisplayName("Event Hubs Connection String")
            .withType(ConfigDef.Type.PASSWORD)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Azure Event Hubs connection string (without EntityPath).");

    public static final Field HUB_NAME = Field.create("hubname")
            .withDisplayName("Event Hub Name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Name of the Event Hub.");

    public static final Field AUTH_MODE = Field.create("authmode")
            .withDisplayName("Authentication Mode")
            .withType(ConfigDef.Type.STRING)
            .withDefault("connection-string")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Authentication mode for Event Hubs. Use 'connection-string' (default) or 'default-azure-credential'.");

    public static final Field FULLY_QUALIFIED_NAMESPACE = Field.create("fullyqualifiednamespace")
            .withDisplayName("Fully Qualified Namespace")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Fully qualified Event Hubs namespace (e.g. <namespace>.servicebus.windows.net). Required when authmode is 'default-azure-credential'.");

    public static final Field PARTITION_ID = Field.create("partitionid")
            .withDisplayName("Partition ID")
            .withType(ConfigDef.Type.STRING)
            .withDefault("")
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Specific partition ID to send all events to. Leave empty for dynamic routing.");

    public static final Field PARTITION_KEY = Field.create("partitionkey")
            .withDisplayName("Partition Key")
            .withType(ConfigDef.Type.STRING)
            .withDefault("")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Partition key to use for all events. Leave empty for dynamic routing.");

    public static final Field DYNAMIC_PARTITION_ROUTING = Field.create("dynamicpartitionrouting")
            .withDisplayName("Dynamic Partition Routing Strategy")
            .withType(ConfigDef.Type.STRING)
            .withDefault("DEFAULT")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Dynamic partition routing strategy when no partition ID or key is configured.");

    public static final Field MAX_BATCH_SIZE = Field.create("maxbatchsize")
            .withDisplayName("Max Batch Size (bytes)")
            .withType(ConfigDef.Type.INT)
            .withDefault(0)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum size in bytes for the batch of events. 0 means use default.");

    public static final Field HASH_MESSAGE_KEY_FUNCTION = Field.create("hashmessagekeyfunction")
            .withDisplayName("Hash Message Key Function")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Hash function to use for message key-based routing.");

    // Instance fields
    private String connectionString;
    private String eventHubName;
    private String authMode;
    private String fullyQualifiedNamespace;
    private String configuredPartitionId;
    private String configuredPartitionKey;
    private String dynamicPartitionRouting;
    private int maxBatchSize;
    private String hashMessageKeyFunction;

    public EventHubsChangeConsumerConfig(Configuration config) {
        init(config);
    }

    protected void init(Configuration config) {
        connectionString = config.getString(CONNECTION_STRING);
        eventHubName = config.getString(HUB_NAME);
        authMode = config.getString(AUTH_MODE);
        fullyQualifiedNamespace = config.getString(FULLY_QUALIFIED_NAMESPACE);
        configuredPartitionId = config.getString(PARTITION_ID);
        configuredPartitionKey = config.getString(PARTITION_KEY);
        dynamicPartitionRouting = config.getString(DYNAMIC_PARTITION_ROUTING);
        maxBatchSize = config.getInteger(MAX_BATCH_SIZE);
        hashMessageKeyFunction = config.getString(HASH_MESSAGE_KEY_FUNCTION);
    }

    public String getConnectionString() {
        return connectionString;
    }

    public String getEventHubName() {
        return eventHubName;
    }

    public String getAuthMode() {
        return authMode;
    }

    public String getFullyQualifiedNamespace() {
        return fullyQualifiedNamespace;
    }

    public String getConfiguredPartitionId() {
        return configuredPartitionId;
    }

    public String getConfiguredPartitionKey() {
        return configuredPartitionKey;
    }

    public String getDynamicPartitionRouting() {
        return dynamicPartitionRouting;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public String getHashMessageKeyFunction() {
        return hashMessageKeyFunction;
    }
}
