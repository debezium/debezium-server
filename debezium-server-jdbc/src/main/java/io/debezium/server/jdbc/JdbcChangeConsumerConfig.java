/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.jdbc;

import java.util.HashMap;
import java.util.Map;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;

/**
 * Configuration wrapper for JDBC sink in Debezium Server.
 *
 * Maps debezium.sink.jdbc.* properties to the format expected by JdbcSinkConnectorConfig.
 * This allows users to configure the JDBC sink using standard Debezium Server conventions
 * while internally using the established JDBC connector configuration.
 *
 * @author Mario Fiore Vitale
 */
public class JdbcChangeConsumerConfig {

    private final JdbcSinkConnectorConfig jdbcConfig;

    /**
     * Creates a new configuration instance from Debezium Server configuration.
     * <p>
     * Transforms properties from debezium.sink.jdbc.* format to the format
     * expected by JdbcSinkConnectorConfig (properties already have prefix removed by getConfigSubset).
     *
     * @param config the Debezium Server configuration (with prefix already removed)
     */
    public JdbcChangeConsumerConfig(Configuration config) {

        Map<String, String> jdbcProps = new HashMap<>();

        config.forEach((key, value) -> {
            if (value != null) {
                jdbcProps.put(key, value.toString());
            }
        });

        this.jdbcConfig = new JdbcSinkConnectorConfig(jdbcProps);
    }

    /**
     * Gets the underlying JDBC connector configuration.
     *
     * @return the JDBC sink connector configuration
     */
    public JdbcSinkConnectorConfig getJdbcConfig() {
        return jdbcConfig;
    }

    /**
     * Gets all configuration fields for the JDBC sink.
     * Used for configuration metadata and documentation.
     *
     * @return set of all configuration fields
     */
    public Field.Set getAllConfigurationFields() {
        return JdbcSinkConnectorConfig.ALL_FIELDS;
    }
}
