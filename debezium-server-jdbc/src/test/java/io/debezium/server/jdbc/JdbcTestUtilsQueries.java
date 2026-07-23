/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.jdbc;

public interface JdbcTestUtilsQueries {

    /**
     * Gets the query for the target record count.
     */
    String targetRecordCountQuery();

    /**
     * Gets the query for checking if the target table exists.
     */
    String targetTableExistsQuery();

    /**
     * Gets the query for fetching all records from the target table.
     */
    String targetRecordsQuery();

    /**
     * Gets the query for fetching a specific record by ID from the target table.
     */
    String targetRecordByIdQuery();

    /**
     * Gets the query for inserting a record into the source database.
     */
    String insertSourceRecordQuery();

    /**
     * Gets the query for updating a record in the source database.
     */
    String updateSourceRecordQuery();

    /**
     * Gets the query for deleting a record from the source database.
     */
    String deleteSourceRecordQuery();

    /**
     * Gets the query for adding a column to the source table for schema evolution testing.
     */
    String addColumnToSourceQuery();

    /**
     * Gets the query for checking if a column exists in the target table.
     */
    String targetColumnExistsQuery();

    /**
     * Gets the query for checking if the target products table exists.
     */
    String targetProductsTableExistsQuery();

    /**
     * Gets the query for getting the count of records in the target products table.
     */
    String targetProductsCountQuery();

    /**
     * Gets the query for checking if a product exists in the target products table.
     */
    String targetProductExistsQuery();

    /**
     * Query for deleting a product in the source database.
     */
    String deleteSourceProductQuery();

    /**
     * Query for deleting all product_on_hand entries for a product_id in the source database.
     */
    String deleteSourceProductOnHandQuery();
}
