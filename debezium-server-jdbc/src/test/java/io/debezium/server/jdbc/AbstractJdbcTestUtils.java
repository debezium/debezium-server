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

/**
 * Utility methods for JDBC integration tests with PostgreSQL source and target.
 */
public abstract class AbstractJdbcTestUtils implements JdbcTestUtils, JdbcTestUtilsQueries {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractJdbcTestUtils.class);

    /**
     * Gets database connection.
     */
    protected abstract Connection getTargetConnection() throws SQLException;

    protected abstract Connection getSourceConnection() throws SQLException;

    /**
     * Gets the number of records in the target table.
     */
    @Override
    public int getTargetRecordCount() throws SQLException {
        try (Connection conn = getTargetConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(targetRecordCountQuery())) {

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
    public boolean targetTableExists() throws SQLException {
        try (Connection conn = getTargetConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(targetTableExistsQuery())) {

            if (rs.next()) {
                return rs.getBoolean(1);
            }
            return false;
        }
    }

    /**
     * Gets all records from the target table.
     */
    public List<Map<String, Object>> getTargetRecords() throws SQLException {
        List<Map<String, Object>> records = new ArrayList<>();

        try (Connection conn = getTargetConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(targetRecordsQuery())) {

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
    public Map<String, Object> getTargetRecordById(int id) throws SQLException {
        try (Connection conn = getTargetConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(String.format(targetRecordByIdQuery(), id))) {

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
    public void insertSourceRecord(int id, String firstName, String lastName, String email) throws SQLException {
        try (Connection conn = getSourceConnection();
                Statement stmt = conn.createStatement()) {

            String sql = String.format(insertSourceRecordQuery(), id, firstName, lastName, email);
            stmt.execute(sql);
            LOGGER.debug("Inserted source record: id={}", id);
        }
    }

    /**
     * Updates a record in the source database.
     */
    public void updateSourceRecord(int id, String firstName, String lastName, String email) throws SQLException {
        try (Connection conn = getSourceConnection();
                Statement stmt = conn.createStatement()) {

            String sql = String.format(updateSourceRecordQuery(), firstName, lastName, email, id);
            stmt.execute(sql);
            LOGGER.debug("Updated source record: id={}", id);
        }
    }

    /**
     * Deletes a record from the source database.
     */
    public void deleteSourceRecord(int id) throws SQLException {
        try (Connection conn = getSourceConnection();
                Statement stmt = conn.createStatement()) {

            String sql = String.format(deleteSourceRecordQuery(), id);
            stmt.execute(sql);
            LOGGER.debug("Deleted source record: id={}", id);
        }
    }

    /**
     * Adds a column to the source table for schema evolution testing.
     */
    public void addColumnToSource(String columnName, String columnType) throws SQLException {
        try (Connection conn = getSourceConnection();
                Statement stmt = conn.createStatement()) {

            String sql = String.format(addColumnToSourceQuery(), columnName, columnType);
            stmt.execute(sql);
            LOGGER.debug("Added column to source: {} {}", columnName, columnType);
        }
    }

    /**
     * Checks if a column exists in the target table.
     */
    public boolean targetColumnExists(String columnName) throws SQLException {
        try (Connection conn = getTargetConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(String.format(targetColumnExistsQuery(), columnName))) {

            if (rs.next()) {
                return rs.getBoolean(1);
            }
            return false;
        }
    }

    /**
     * Checks if target products table exists.
     */
    public boolean targetProductsTableExists() throws SQLException {
        try (Connection conn = getTargetConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(targetProductsTableExistsQuery())) {

            if (rs.next()) {
                return rs.getBoolean(1);
            }
            return false;
        }
    }

    /**
     * Gets the number of records in the target products table.
     */
    public int getTargetProductsCount() throws SQLException {
        try (Connection conn = getTargetConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(targetProductsCountQuery())) {

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
    public boolean targetProductExists(int id) throws SQLException {
        try (Connection conn = getTargetConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(String.format(targetProductExistsQuery(), id))) {

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
    public void deleteSourceProduct(int id) throws SQLException {
        try (Connection conn = getSourceConnection();
                Statement stmt = conn.createStatement()) {

            // Delete from products_on_hand first to avoid FK constraint
            String sql1 = String.format(deleteSourceProductOnHandQuery(), id);
            stmt.execute(sql1);
            LOGGER.debug("Deleted from products_on_hand for product: id={}", id);

            // Then delete the product
            String sql2 = String.format(deleteSourceProductQuery(), id);
            stmt.execute(sql2);
            LOGGER.debug("Deleted source product: id={}", id);
        }
    }
}
