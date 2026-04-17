/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/**
 * Manages lifecycle of a PostgreSQL container used as the JDBC sink target.
 * This is separate from the source PostgreSQL managed by PostgresTestResourceLifecycleManager.
 */
public class JdbcTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcTestResourceLifecycleManager.class);

    public static PostgreSQLContainer<?> container;

    @Override
    public Map<String, String> start() {
        try {
            container = new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("target_db")
                    .withUsername("postgres")
                    .withPassword("postgres");

            container.start();

            String jdbcUrl = container.getJdbcUrl();
            LOGGER.info("JDBC sink target container started at: {}", jdbcUrl);

            Map<String, String> config = new ConcurrentHashMap<>();
            config.put("debezium.sink.jdbc.connection.url", jdbcUrl);
            config.put("debezium.sink.jdbc.connection.username", container.getUsername());
            config.put("debezium.sink.jdbc.connection.password", container.getPassword());

            return config;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to start JDBC target container", e);
        }
    }

    @Override
    public void stop() {
        try {
            if (container != null) {
                container.stop();
                LOGGER.info("JDBC sink target container stopped");
            }
        }
        catch (Exception e) {
            LOGGER.warn("Error stopping JDBC target container", e);
        }
    }

    /**
     * Gets a connection to the target database.
     */
    public static Connection getTargetConnection() throws SQLException {
        return DriverManager.getConnection(
                container.getJdbcUrl(),
                container.getUsername(),
                container.getPassword());
    }

    /**
     * Executes a SQL statement on the target database.
     */
    public static void executeSql(String sql) throws SQLException {
        try (Connection conn = getTargetConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    /**
     * Cleans up test tables in the target database.
     */
    public static void cleanupTables() {
        try {
            executeSql("DROP TABLE IF EXISTS testserver_inventory_customers CASCADE");
            LOGGER.debug("Cleaned up target tables");
        }
        catch (Exception e) {
            LOGGER.warn("Error cleaning up tables", e);
        }
    }
}
