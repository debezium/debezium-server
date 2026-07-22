/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;

/**
 * Utility methods for JDBC integration tests with PostgreSQL source and target.
 */
public class JdbcPostgresTestUtils extends AbstractJdbcTestUtils implements JdbcTestUtilsQueries {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcPostgresTestUtils.class);

    private static final String TARGET_TABLE = "testserver_inventory_customers";
    private static final String TARGET_PRODUCTS_TABLE = "testserver_inventory_products";

    @Override
    protected Connection getSourceConnection() throws SQLException {
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

    @Override
    protected Connection getTargetConnection() throws SQLException {
        return JdbcPostgresResourceLifecycleManager.getTargetConnection();
    }

    @Override
    public String targetRecordCountQuery() {
        return "SELECT COUNT(*) FROM " + TARGET_TABLE;
    }

    @Override
    public String targetTableExistsQuery() {
        return "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '" + TARGET_TABLE + "')";
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
        return "INSERT INTO inventory.customers (id, first_name, last_name, email) VALUES (%d, '%s', '%s', '%s')";
    }

    @Override
    public String updateSourceRecordQuery() {
        return "UPDATE inventory.customers SET first_name='%s', last_name='%s', email='%s' WHERE id=%d";
    }

    @Override
    public String deleteSourceRecordQuery() {
        return "DELETE FROM inventory.customers WHERE id=%d";
    }

    @Override
    public String addColumnToSourceQuery() {
        return "ALTER TABLE inventory.customers ADD COLUMN IF NOT EXISTS %s %s";
    }

    @Override
    public String targetColumnExistsQuery() {
        return "SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_name = '" + TARGET_TABLE + "' AND column_name = '%s')";
    }

    @Override
    public String targetProductsTableExistsQuery() {
        return "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '" + TARGET_PRODUCTS_TABLE + "')";
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
        return "DELETE FROM inventory.products WHERE id = %d";
    }

    @Override
    public String deleteSourceProductOnHandQuery() {
        return "DELETE FROM inventory.products_on_hand WHERE product_id = %d";
    }

}
