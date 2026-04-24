/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.fluss;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link FlussChangeConsumer}.
 *
 * @author Chris Cranford
 */
public class FlussChangeConsumerConfig {

    public static final int DEFAULT_RETRY_COUNT = 5;

    public static final Field BOOTSTRAP_SERVERS = Field.create("bootstrap.servers")
            .withDisplayName("Bootstrap Servers")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Comma-separated list of Fluss coordinator server addresses (host:port).");

    public static final Field DEFAULT_DATABASE = Field.create("default.database")
            .withDisplayName("Default Database")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Default Fluss database used when resolving table paths from event destinations.");

    public static final Field TABLE_AUTO_CREATE = Field.create("table.auto.create")
            .withDisplayName("Auto-create Tables")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("When true, automatically create Fluss tables if they do not exist, "
                    + "deriving the schema from the Debezium event schema. Requires the JSON converter "
                    + "with schemas.enable=true.");

    public static final Field DEFAULT_RETRIES = Field.create("default.retries")
            .withDisplayName("Default Retries")
            .withType(ConfigDef.Type.INT)
            .withDefault(DEFAULT_RETRY_COUNT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum number of retry attempts on transient write failures.");

    private final String bootstrapServers;
    private final String defaultDatabase;
    private final boolean tableAutoCreate;
    private final int maxRetries;

    public FlussChangeConsumerConfig(Configuration config) {
        bootstrapServers = config.getString(BOOTSTRAP_SERVERS);
        defaultDatabase = config.getString(DEFAULT_DATABASE);
        tableAutoCreate = config.getBoolean(TABLE_AUTO_CREATE);
        maxRetries = config.getInteger(DEFAULT_RETRIES);
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    public boolean isTableAutoCreate() {
        return tableAutoCreate;
    }

    public int getMaxRetries() {
        return maxRetries;
    }
}