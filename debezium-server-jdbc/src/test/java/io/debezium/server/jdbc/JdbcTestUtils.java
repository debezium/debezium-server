/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;

/**
 * Utility methods for JDBC integration tests.
 */
public class JdbcTestUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcTestUtils.class);

    private static final String TARGET_TABLE = "testserver_inventory_customers";
    private static final String TARGET_PRODUCTS_TABLE = "testserver_inventory_products";

    /**
     * Gets the number of records in the target table.
     */
    public static int getTargetRecordCount() throws SQLException {
        try (Connection conn = JdbcTestResourceLifecycleManager.getTargetConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TARGET_TABLE)) {

            if (rs.next()) {
                int count = rs.getInt(1);
                LOGGER.debug("Target table has {} records", count);
                return count;
            }
            return 0;
        }
    }

    /**
     * Checks if target table exists.
     */
    public static boolean targetTableExists() throws SQLException {
        try (Connection conn = JdbcTestResourceLifecycleManager.getTargetConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '" + TARGET_TABLE + "')")) {

            if (rs.next()) {
                return rs.getBoolean(1);
            }
            return false;
        }
    }

    /**
     * Gets all records from the target table.
     */
    public static List<Map<String, Object>> getTargetRecords() throws SQLException {
        List<Map<String, Object>> records = new ArrayList<>();

        try (Connection conn = JdbcTestResourceLifecycleManager.getTargetConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + TARGET_TABLE + " ORDER BY id")) {

            while (rs.next()) {
                Map<String, Object> record = new HashMap<>();
                record.put("id", rs.getInt("id"));
                record.put("first_name", rs.getString("first_name"));
                record.put("last_name", rs.getString("last_name"));
                record.put("email", rs.getString("email"));
                records.add(record);
            }
        }

        LOGGER.debug("Retrieved {} records from target", records.size());
        return records;
    }

    /**
     * Gets a specific record by ID from the target table.
     */
    public static Map<String, Object> getTargetRecordById(int id) throws SQLException {
        try (Connection conn = JdbcTestResourceLifecycleManager.getTargetConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + TARGET_TABLE + " WHERE id = " + id)) {

            if (rs.next()) {
                Map<String, Object> record = new HashMap<>();
                record.put("id", rs.getInt("id"));
                record.put("first_name", rs.getString("first_name"));
                record.put("last_name", rs.getString("last_name"));
                record.put("email", rs.getString("email"));
                return record;
            }
            return null;
        }
    }

    /**
     * Inserts a record into the source database.
     */
    public static void insertSourceRecord(int id, String firstName, String lastName, String email) throws SQLException {
        try (Connection conn = getSourceConnection();
                Statement stmt = conn.createStatement()) {

            String sql = String.format(
                    "INSERT INTO inventory.customers (id, first_name, last_name, email) VALUES (%d, '%s', '%s', '%s')",
                    id, firstName, lastName, email);
            stmt.execute(sql);
            LOGGER.debug("Inserted source record: id={}", id);
        }
    }

    /**
     * Updates a record in the source database.
     */
    public static void updateSourceRecord(int id, String firstName, String lastName, String email) throws SQLException {
        try (Connection conn = getSourceConnection();
                Statement stmt = conn.createStatement()) {

            String sql = String.format(
                    "UPDATE inventory.customers SET first_name='%s', last_name='%s', email='%s' WHERE id=%d",
                    firstName, lastName, email, id);
            stmt.execute(sql);
            LOGGER.debug("Updated source record: id={}", id);
        }
    }

    /**
     * Deletes a record from the source database.
     */
    public static void deleteSourceRecord(int id) throws SQLException {
        try (Connection conn = getSourceConnection();
                Statement stmt = conn.createStatement()) {

            String sql = String.format("DELETE FROM inventory.customers WHERE id=%d", id);
            stmt.execute(sql);
            LOGGER.debug("Deleted source record: id={}", id);
        }
    }

    /**
     * Adds a column to the source table for schema evolution testing.
     */
    public static void addColumnToSource(String columnName, String columnType) throws SQLException {
        try (Connection conn = getSourceConnection();
                Statement stmt = conn.createStatement()) {

            String sql = String.format(
                    "ALTER TABLE inventory.customers ADD COLUMN IF NOT EXISTS %s %s",
                    columnName, columnType);
            stmt.execute(sql);
            LOGGER.debug("Added column to source: {} {}", columnName, columnType);
        }
    }

    /**
     * Checks if a column exists in the target table.
     */
    public static boolean targetColumnExists(String columnName) throws SQLException {
        try (Connection conn = JdbcTestResourceLifecycleManager.getTargetConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT EXISTS (SELECT FROM information_schema.columns " +
                                "WHERE table_name = '" + TARGET_TABLE + "' AND column_name = '" + columnName + "')")) {

            if (rs.next()) {
                return rs.getBoolean(1);
            }
            return false;
        }
    }

    /**
     * Gets a connection to the source database.
     */
    private static Connection getSourceConnection() throws SQLException {
        var container = PostgresTestResourceLifecycleManager.getContainer();
        String jdbcUrl = String.format(
                "jdbc:postgresql://%s:%d/%s",
                PostgresTestResourceLifecycleManager.POSTGRES_HOST,
                container.getMappedPort(PostgresTestResourceLifecycleManager.POSTGRES_PORT),
                PostgresTestResourceLifecycleManager.POSTGRES_DBNAME);

        return java.sql.DriverManager.getConnection(
                jdbcUrl,
                PostgresTestResourceLifecycleManager.POSTGRES_USER,
                PostgresTestResourceLifecycleManager.POSTGRES_PASSWORD);
    }

    /**
     * Checks if target products table exists.
     */
    public static boolean targetProductsTableExists() throws SQLException {
        try (Connection conn = JdbcTestResourceLifecycleManager.getTargetConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '" + TARGET_PRODUCTS_TABLE + "')")) {

            if (rs.next()) {
                return rs.getBoolean(1);
            }
            return false;
        }
    }

    /**
     * Gets the number of records in the target products table.
     */
    public static int getTargetProductsCount() throws SQLException {
        try (Connection conn = JdbcTestResourceLifecycleManager.getTargetConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TARGET_PRODUCTS_TABLE)) {

            if (rs.next()) {
                int count = rs.getInt(1);
                LOGGER.debug("Target products table has {} records", count);
                return count;
            }
            return 0;
        }
    }

    /**
     * Checks if a product exists in target by ID.
     */
    public static boolean targetProductExists(int id) throws SQLException {
        try (Connection conn = JdbcTestResourceLifecycleManager.getTargetConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TARGET_PRODUCTS_TABLE + " WHERE id = " + id)) {

            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
            return false;
        }
    }

    /**
     * Deletes a product from the source database.
     * Also deletes from products_on_hand to avoid foreign key constraint violations.
     */
    public static void deleteSourceProduct(int id) throws SQLException {
        try (Connection conn = getSourceConnection();
                Statement stmt = conn.createStatement()) {

            // Delete from products_on_hand first to avoid FK constraint
            String sql1 = String.format("DELETE FROM inventory.products_on_hand WHERE product_id=%d", id);
            stmt.execute(sql1);
            LOGGER.debug("Deleted from products_on_hand for product: id={}", id);

            // Then delete the product
            String sql2 = String.format("DELETE FROM inventory.products WHERE id=%d", id);
            stmt.execute(sql2);
            LOGGER.debug("Deleted source product: id={}", id);
        }
    }
}
