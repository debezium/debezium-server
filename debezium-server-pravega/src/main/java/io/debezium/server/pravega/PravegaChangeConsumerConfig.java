/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pravega;

import java.net.URI;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link PravegaChangeConsumer}.
 */
public class PravegaChangeConsumerConfig {

    public static final Field CONTROLLER_URI = Field.create("controller.uri")
            .withDisplayName("Controller URI")
            .withType(ConfigDef.Type.STRING)
            .withDefault("tcp://localhost:9090")
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Pravega controller URI.");

    public static final Field SCOPE = Field.create("scope")
            .withDisplayName("Scope")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Pravega scope.");

    public static final Field TRANSACTION = Field.create("transaction")
            .withDisplayName("Transaction")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Enable transaction support.");

    // Instance fields
    private URI controllerUri;
    private String scope;
    private boolean transaction;

    public PravegaChangeConsumerConfig(Configuration config) {
        init(config);
    }

    protected void init(Configuration config) {
        String uriStr = config.getString(CONTROLLER_URI);
        controllerUri = URI.create(uriStr);
        scope = config.getString(SCOPE);
        transaction = config.getBoolean(TRANSACTION, false);
    }

    public URI getControllerUri() {
        return controllerUri;
    }

    public String getScope() {
        return scope;
    }

    public boolean isTransaction() {
        return transaction;
    }
}
