/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pulsar;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link PulsarChangeConsumer}.
 */
public class PulsarChangeConsumerConfig {

    public static final Field NULL_KEY = Field.create("null.key")
            .withDisplayName("Null Key Value")
            .withType(ConfigDef.Type.STRING)
            .withDefault("default")
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Key to use when the record key is null.");

    public static final Field TENANT = Field.create("tenant")
            .withDisplayName("Pulsar Tenant")
            .withType(ConfigDef.Type.STRING)
            .withDefault("public")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Pulsar tenant name.");

    public static final Field NAMESPACE = Field.create("namespace")
            .withDisplayName("Pulsar Namespace")
            .withType(ConfigDef.Type.STRING)
            .withDefault("default")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Pulsar namespace name.");

    public static final Field TIMEOUT = Field.create("timeout")
            .withDisplayName("Timeout (ms)")
            .withType(ConfigDef.Type.INT)
            .withDefault(0)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Timeout in milliseconds for producer close operations. 0 means no timeout.");

    public static final Field PRODUCER_BATCHER_BUILDER = Field.create("producer.batcherBuilder")
            .withDisplayName("Batcher Builder")
            .withType(ConfigDef.Type.STRING)
            .withDefault("DEFAULT")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Pulsar batcher builder type: DEFAULT, KEY_BASED.");

    // Instance fields
    private String nullKey;
    private String pulsarTenant;
    private String pulsarNamespace;
    private int timeout;
    private String batcherBuilder;

    public PulsarChangeConsumerConfig(Configuration config) {
        init(config);
    }

    protected void init(Configuration config) {
        nullKey = config.getString(NULL_KEY);
        pulsarTenant = config.getString(TENANT);
        pulsarNamespace = config.getString(NAMESPACE);
        timeout = config.getInteger(TIMEOUT);
        batcherBuilder = config.getString(PRODUCER_BATCHER_BUILDER);
    }

    public String getNullKey() {
        return nullKey;
    }

    public String getPulsarTenant() {
        return pulsarTenant;
    }

    public String getPulsarNamespace() {
        return pulsarNamespace;
    }

    public int getTimeout() {
        return timeout;
    }

    public String getBatcherBuilder() {
        return batcherBuilder;
    }
}
