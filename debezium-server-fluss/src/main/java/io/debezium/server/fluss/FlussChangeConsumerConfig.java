/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.fluss;

import java.time.Duration;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link FlussChangeConsumer}.
 *
 * @author Chris Cranford
 */
public class FlussChangeConsumerConfig {

    public enum PrimaryKeyMode implements EnumeratedValue {
        AUTO("auto"),
        UPSERT("upsert"),
        APPEND("append");

        private final String value;

        PrimaryKeyMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static PrimaryKeyMode parse(String value) {
            for (PrimaryKeyMode mode : values()) {
                if (mode.getValue().equalsIgnoreCase(value)) {
                    return mode;
                }
            }
            throw new DebeziumException("Invalid primary.key.mode '" + value + "'. Must be one of: auto, upsert, append.");
        }
    }

    public static final int DEFAULT_RETRY_COUNT = 5;
    public static final long DEFAULT_RETRY_INITIAL_INTERVAL_MS = 1000;
    public static final long DEFAULT_RETRY_MAX_INTERVAL_MS = 60_000;
    public static final double DEFAULT_RETRY_BACKOFF_MULTIPLIER = 2.0;

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

    public static final Field PRIMARY_KEY_MODE = Field.create("primary.key.mode")
            .withDisplayName("Primary Key Mode")
            .withType(ConfigDef.Type.STRING)
            .withDefault("auto")
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Controls how the sink selects the write mode for each table. "
                    + "'auto' (default) switches automatically based on whether the target table has a primary key; "
                    + "'upsert' requires the table to have a primary key, and throws an error if it does not; "
                    + "'append' requires the table to have no primary key, and throws an error if it does.");

    public static final Field TABLE_AUTO_CREATE = Field.create("table.auto.create")
            .withDisplayName("Auto-create Tables")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("When true, automatically create Fluss tables if they do not exist, "
                    + "deriving the schema from the Debezium event schema. Requires the JSON converter "
                    + "with schemas.enable=true.");

    public static final Field RETRIES_MAX = Field.create("retries.max")
            .withDisplayName("Maximum Retries")
            .withType(ConfigDef.Type.INT)
            .withDefault(DEFAULT_RETRY_COUNT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum number of retry attempts on transient write failures.");

    public static final Field RETRIES_INTERVAL_MS = Field.create("retries.interval.ms")
            .withDisplayName("Retry Interval")
            .withType(ConfigDef.Type.INT)
            .withDefault(DEFAULT_RETRY_INITIAL_INTERVAL_MS)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("The initial retry interval in milliseconds.");

    public static final Field RETRIES_MAX_INTERVAL_MS = Field.create("retries.max.interval.ms")
            .withDisplayName("Retry Maximum Interval")
            .withType(ConfigDef.Type.INT)
            .withDefault(DEFAULT_RETRY_MAX_INTERVAL_MS)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("The maximum retry interval in milliseconds.");

    public static final Field RETRIES_BACKOFF_MULTIPLIER = Field.create("retries.backoff.multiplier")
            .withDisplayName("Retry Backoff Multiplier")
            .withType(ConfigDef.Type.DOUBLE)
            .withDefault(String.valueOf(DEFAULT_RETRY_BACKOFF_MULTIPLIER))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Backoff multiplier for retry intervals.");

    private final String bootstrapServers;
    private final String defaultDatabase;
    private final PrimaryKeyMode primaryKeyMode;
    private final boolean tableAutoCreate;
    private final int maxRetries;
    private final Duration retryInterval;
    private final Duration retryMaxInterval;
    private final double retryBackoffMultiplier;

    public FlussChangeConsumerConfig(Configuration config) {
        bootstrapServers = config.getString(BOOTSTRAP_SERVERS);
        defaultDatabase = config.getString(DEFAULT_DATABASE);
        primaryKeyMode = PrimaryKeyMode.parse(config.getString(PRIMARY_KEY_MODE));
        tableAutoCreate = config.getBoolean(TABLE_AUTO_CREATE);
        maxRetries = config.getInteger(RETRIES_MAX);
        retryInterval = Duration.ofMillis(config.getLong(RETRIES_INTERVAL_MS));
        retryMaxInterval = Duration.ofMillis(config.getLong(RETRIES_MAX_INTERVAL_MS));
        retryBackoffMultiplier = Double.parseDouble(config.getString(RETRIES_BACKOFF_MULTIPLIER));
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    public PrimaryKeyMode getPrimaryKeyMode() {
        return primaryKeyMode;
    }

    public boolean isTableAutoCreate() {
        return tableAutoCreate;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public Duration getRetryInterval() {
        return retryInterval;
    }

    public Duration getRetryMaxInterval() {
        return retryMaxInterval;
    }

    public double getDefaultRetryBackoffMultiplier() {
        return retryBackoffMultiplier;
    }
}