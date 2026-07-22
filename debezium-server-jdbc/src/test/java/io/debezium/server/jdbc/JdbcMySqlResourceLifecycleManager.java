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
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.server.Images;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/**
 * Manages lifecycle of a MySQL container used as the JDBC sink target.
 * This is separate from the source MySQL managed by MySqlTestResourceLifecycleManager.
 */
public class JdbcMySqlResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcMySqlResourceLifecycleManager.class);

    public static MySQLContainer container;

    @Override
    public Map<String, String> start() {
        try {
            container = new MySQLContainer(DockerImageName.parse(Images.MYSQL_IMAGE).asCompatibleSubstituteFor("mysql"))
                    .withDatabaseName("target_db")
                    .withUsername("target_user")
                    .withPassword("target_password");

            container.start();

            String jdbcUrl = container.getJdbcUrl();
            LOGGER.info("JDBC sink target MySQL container started at: {}", jdbcUrl);

            Map<String, String> config = new ConcurrentHashMap<>();
            config.put("debezium.sink.jdbc.connection.url", jdbcUrl);
            config.put("debezium.sink.jdbc.connection.username", container.getUsername());
            config.put("debezium.sink.jdbc.connection.password", container.getPassword());

            return config;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to start JDBC target MySQL container", e);
        }
    }

    @Override
    public void stop() {
        try {
            if (container != null) {
                container.stop();
                LOGGER.info("JDBC sink target MySQL container stopped");
            }
        }
        catch (Exception e) {
            LOGGER.warn("Error stopping JDBC target MySQL container", e);
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
}
