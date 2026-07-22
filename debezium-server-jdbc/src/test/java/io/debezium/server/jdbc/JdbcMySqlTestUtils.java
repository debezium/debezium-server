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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.testcontainers.MySqlTestResourceLifecycleManager;

/**
 * Utility methods for JDBC integration tests with MySQL source and target.
 */
public class JdbcMySqlTestUtils extends AbstractJdbcTestUtils implements JdbcTestUtilsQueries {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcMySqlTestUtils.class);

    private static final String TARGET_TABLE = "testserver_inventory_customers";
    private static final String TARGET_PRODUCTS_TABLE = "testserver_inventory_products";

    /**
     * Gets a connection to the source database.
     */
    @Override
    protected Connection getSourceConnection() throws SQLException {
        var container = MySqlTestResourceLifecycleManager.getContainer();
        String jdbcUrl = String.format(
                "jdbc:mysql://%s:%d/%s",
                MySqlTestResourceLifecycleManager.HOST,
                container.getMappedPort(MySqlTestResourceLifecycleManager.PORT),
                MySqlTestResourceLifecycleManager.DBNAME);

        return DriverManager.getConnection(
                jdbcUrl,
                MySqlTestResourceLifecycleManager.PRIVILEGED_USER,
                MySqlTestResourceLifecycleManager.PRIVILEGED_PASSWORD);
    }

    @Override
    protected Connection getTargetConnection() throws SQLException {
        return JdbcMySqlResourceLifecycleManager.getTargetConnection();
    }

    @Override
    public String targetRecordCountQuery() {
        return "SELECT COUNT(*) FROM " + TARGET_TABLE;
    }

    @Override
    public String targetTableExistsQuery() {
        return "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '" + TARGET_TABLE + "'";
    }

    @Override
    public String targetRecordsQuery() {
        return "SELECT * FROM " + TARGET_TABLE + " ORDER BY id";
    }

    @Override
    public String targetRecordByIdQuery() {
        return "SELECT * FROM " + TARGET_TABLE + " WHERE id = %d";
    }

    @Override
    public String insertSourceRecordQuery() {
        return "INSERT INTO customers (id, first_name, last_name, email) VALUES (%d, '%s', '%s', '%s')";
    }

    @Override
    public String updateSourceRecordQuery() {
        return "UPDATE customers SET first_name = '%s', last_name = '%s', email = '%s' WHERE id = %d";
    }

    @Override
    public void deleteSourceRecord(int id) throws SQLException {
        try (Connection conn = getSourceConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("DELETE FROM addresses WHERE customer_id = %d", id));
            stmt.execute(String.format(deleteSourceRecordQuery(), id));
        }
    }

    @Override
    public String deleteSourceRecordQuery() {
        return "DELETE FROM customers WHERE id = %d";
    }

    @Override
    public String addColumnToSourceQuery() {
        return "ALTER TABLE customers ADD COLUMN %s %s";
    }

    @Override
    public String targetColumnExistsQuery() {
        return "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = '" + TARGET_TABLE + "' AND column_name = '%s'";
    }

    @Override
    public String targetProductsTableExistsQuery() {
        return "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '" + TARGET_PRODUCTS_TABLE + "'";
    }

    @Override
    public String targetProductsCountQuery() {
        return "SELECT COUNT(*) FROM " + TARGET_PRODUCTS_TABLE;
    }

    @Override
    public String targetProductExistsQuery() {
        return "SELECT COUNT(*) FROM " + TARGET_PRODUCTS_TABLE + " WHERE id = %d";
    }

    @Override
    public String deleteSourceProductQuery() {
        return "DELETE FROM products WHERE id = %d";
    }

    @Override
    public String deleteSourceProductOnHandQuery() {
        return "DELETE FROM products_on_hand WHERE product_id = %d";
    }

}
