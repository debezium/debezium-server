/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pubsub;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link PubSubLiteChangeConsumer}.
 */
public class PubSubLiteChangeConsumerConfig {

    public static final Field PROJECT_ID = Field.create("project.id")
            .withDisplayName("GCP Project ID")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Google Cloud project ID. If not specified, uses the default from the environment.");

    public static final Field REGION = Field.create("region")
            .withDisplayName("GCP Region or Zone")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Google Cloud region or zone for the Pub/Sub Lite topic (e.g., 'us-central1-a').");

    public static final Field ORDERING_ENABLED = Field.create("ordering.enabled")
            .withDisplayName("Message Ordering Enabled")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(true)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Enable message ordering to preserve order within a single ordering key.");

    public static final Field NULL_KEY = Field.create("null.key")
            .withDisplayName("Null Key Value")
            .withType(ConfigDef.Type.STRING)
            .withDefault("default")
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Ordering key to use when the record key is null.");

    public static final Field WAIT_MESSAGE_DELIVERY_TIMEOUT_MS = Field.create("wait.message.delivery.timeout.ms")
            .withDisplayName("Wait Message Delivery Timeout (ms)")
            .withType(ConfigDef.Type.INT)
            .withDefault(30000)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Timeout in milliseconds to wait for message delivery confirmation.");

    // Instance fields
    private String projectId;
    private String region;
    private boolean orderingEnabled;
    private String nullKey;
    private int waitMessageDeliveryTimeout;

    public PubSubLiteChangeConsumerConfig(Configuration config) {
        init(config);
    }

    protected void init(Configuration config) {
        projectId = config.getString(PROJECT_ID);
        region = config.getString(REGION);
        orderingEnabled = config.getBoolean(ORDERING_ENABLED);
        nullKey = config.getString(NULL_KEY);
        waitMessageDeliveryTimeout = config.getInteger(WAIT_MESSAGE_DELIVERY_TIMEOUT_MS);
    }

    public String getProjectId() {
        return projectId;
    }

    public String getRegion() {
        return region;
    }

    public boolean isOrderingEnabled() {
        return orderingEnabled;
    }

    public String getNullKey() {
        return nullKey;
    }

    public int getWaitMessageDeliveryTimeout() {
        return waitMessageDeliveryTimeout;
    }
}
