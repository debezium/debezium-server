/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.jdbc;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface JdbcTestUtils {

    /**
     * Gets the number of records in the target table.
     */
    int getTargetRecordCount() throws SQLException;

    /**
     * Checks if target table exists.
     */
    boolean targetTableExists() throws SQLException;

    /**
     * Gets all records from the target table.
     */
    List<Map<String, Object>> getTargetRecords() throws SQLException;

    /**
     * Gets a specific record by ID from the target table.
     */
    Map<String, Object> getTargetRecordById(int id) throws SQLException;

    /**
     * Inserts a record into the source database.
     */
    void insertSourceRecord(int id, String firstName, String lastName, String email) throws SQLException;

    /**
     * Updates a record in the source database.
     */
    void updateSourceRecord(int id, String firstName, String lastName, String email) throws SQLException;

    /**
     * Deletes a record from the source database.
     */
    void deleteSourceRecord(int id) throws SQLException;

    /**
     * Adds a column to the source table for schema evolution testing.
     */
    void addColumnToSource(String columnName, String columnType) throws SQLException;

    /**
     * Checks if a column exists in the target table.
     */
    boolean targetColumnExists(String columnName) throws SQLException;

    /**
     * Checks if target products table exists.
     */
    boolean targetProductsTableExists() throws SQLException;

    /**
     * Gets the number of records in the target products table.
     */
    int getTargetProductsCount() throws SQLException;

    /**
     * Checks if a product exists in target by ID.
     */
    boolean targetProductExists(int id) throws SQLException;

    /**
     * Deletes a product from the source database.
     * Also deletes from products_on_hand to avoid foreign key constraint violations.
     */
    void deleteSourceProduct(int id) throws SQLException;

}
