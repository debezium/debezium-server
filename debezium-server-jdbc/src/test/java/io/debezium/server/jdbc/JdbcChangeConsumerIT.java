/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import jakarta.enterprise.event.Observes;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.runtime.events.ConnectorStartedEvent;
import io.debezium.runtime.events.DebeziumCompletionEvent;
import io.debezium.server.TestConfigSource;
import io.debezium.util.Testing;

/**
 * Integration tests for JDBC sink with PostgreSQL source and target.
 * <p>
 * Tests cover:
 * - Initial snapshot replication
 * - INSERT operations
 * - UPDATE operations (upsert mode)
 * - DELETE operations
 * - Schema evolution
 */
public abstract class JdbcChangeConsumerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcChangeConsumerIT.class);
    private static final int INITIAL_RECORD_COUNT = 4;

    protected abstract JdbcTestUtils testUtils();

    @BeforeAll
    static void setupOffsetFile() {
        LOGGER.info("Setting up offset file before all tests");
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(TestConfigSource.OFFSET_STORE_PATH);
    }

    void connectorCompleted(@Observes DebeziumCompletionEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError();
        }
    }

    void connectorStarted(@Observes ConnectorStartedEvent event) {
        LOGGER.info("Connector started event received");
    }

    @Test
    @Order(1)
    public void testInitialSnapshot() throws Exception {
        Testing.Print.enable();

        LOGGER.info("Waiting for initial snapshot to complete...");

        // Wait for initial snapshot records to be replicated to target
        Awaitility.await()
                .atMost(Duration.ofSeconds(120))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    try {
                        if (!testUtils().targetTableExists()) {
                            LOGGER.debug("Target table does not exist yet");
                            return false;
                        }

                        int count = testUtils().getTargetRecordCount();
                        LOGGER.debug("Target table has {} records", count);
                        return count >= INITIAL_RECORD_COUNT;
                    }
                    catch (Exception e) {
                        LOGGER.debug("Error checking target table: {}", e.getMessage());
                        return false;
                    }
                });

        // Verify the actual data
        List<Map<String, Object>> records = testUtils().getTargetRecords();
        assertThat(records).hasSize(INITIAL_RECORD_COUNT);

        // Verify first record (Sally Thomas from PostgresTestResourceLifecycleManager)
        Map<String, Object> firstRecord = records.get(0);
        assertThat(firstRecord.get("id")).isEqualTo(1001);
        assertThat(firstRecord.get("first_name")).isEqualTo("Sally");
        assertThat(firstRecord.get("last_name")).isEqualTo("Thomas");
        assertThat(firstRecord.get("email")).isEqualTo("sally.thomas@acme.com");

        LOGGER.info("Initial snapshot test passed - {} records replicated", records.size());
    }

    @Test
    @Order(2)
    public void testInsertOperation() throws Exception {
        Testing.Print.enable();

        LOGGER.info("Inserting new record into source database...");

        // Insert a new record in the source
        testUtils().insertSourceRecord(
                2001,
                "John",
                "Doe",
                "john.doe@example.com");

        // Wait for the insert to be replicated
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    try {
                        int count = testUtils().getTargetRecordCount();
                        LOGGER.debug("Target has {} records (expecting {})", count, INITIAL_RECORD_COUNT + 1);
                        return count == INITIAL_RECORD_COUNT + 1;
                    }
                    catch (Exception e) {
                        LOGGER.debug("Error: {}", e.getMessage());
                        return false;
                    }
                });

        // Verify the new record
        Map<String, Object> newRecord = testUtils().getTargetRecordById(2001);
        assertThat(newRecord).isNotNull();
        assertThat(newRecord.get("first_name")).isEqualTo("John");
        assertThat(newRecord.get("last_name")).isEqualTo("Doe");
        assertThat(newRecord.get("email")).isEqualTo("john.doe@example.com");

        LOGGER.info("Insert operation test passed");
    }

    @Test
    @Order(3)
    public void testUpdateOperation() throws Exception {
        Testing.Print.enable();

        LOGGER.info("Updating existing record in source database...");

        // Update an existing record
        testUtils().updateSourceRecord(
                1001,
                "Sally Updated",
                "Thomas Updated",
                "sally.updated@acme.com");

        // Wait for the update to be replicated
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    try {
                        Map<String, Object> record = testUtils().getTargetRecordById(1001);
                        if (record == null) {
                            return false;
                        }
                        boolean updated = "Sally Updated".equals(record.get("first_name"));
                        LOGGER.debug("Record 1001 first_name: {} (updated: {})",
                                record.get("first_name"), updated);
                        return updated;
                    }
                    catch (Exception e) {
                        LOGGER.debug("Error: {}", e.getMessage());
                        return false;
                    }
                });

        // Verify the updated record
        Map<String, Object> updatedRecord = testUtils().getTargetRecordById(1001);
        assertThat(updatedRecord).isNotNull();
        assertThat(updatedRecord.get("first_name")).isEqualTo("Sally Updated");
        assertThat(updatedRecord.get("last_name")).isEqualTo("Thomas Updated");
        assertThat(updatedRecord.get("email")).isEqualTo("sally.updated@acme.com");

        LOGGER.info("Update operation test passed");
    }

    @Test
    @Order(4)
    public void testDeleteOperation() throws Exception {
        Testing.Print.enable();

        LOGGER.info("Deleting product from source database...");

        int initialCount = testUtils().getTargetProductsCount();
        LOGGER.info("Initial products count: {}", initialCount);

        // Delete a product (ID 101 exists in the example-postgres image)
        // Products table has no foreign key constraints
        testUtils().deleteSourceProduct(101);

        // Wait for the delete to be replicated
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    try {
                        int count = testUtils().getTargetProductsCount();
                        LOGGER.debug("Target has {} products (expecting {})", count, initialCount - 1);
                        return count == initialCount - 1;
                    }
                    catch (Exception e) {
                        LOGGER.debug("Error: {}", e.getMessage());
                        return false;
                    }
                });

        // Verify the product is gone
        boolean productExists = testUtils().targetProductExists(101);
        assertThat(productExists).isFalse();

        // Verify total count
        assertThat(testUtils().getTargetProductsCount()).isEqualTo(initialCount - 1);

        LOGGER.info("Delete operation test passed");
    }

    @Test
    @Order(5)
    public void testSchemaEvolution() throws Exception {
        Testing.Print.enable();

        LOGGER.info("Adding new column to source table...");

        // Add a new column to the source table
        testUtils().addColumnToSource("phone_number", "VARCHAR(50)");

        // Insert a record with the new column
        testUtils().insertSourceRecord(
                3001,
                "Jane",
                "Smith",
                "jane.smith@example.com");

        // Wait for the record to be replicated (which should trigger column addition)
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    try {
                        return testUtils().getTargetRecordById(3001) != null;
                    }
                    catch (Exception e) {
                        return false;
                    }
                });

        // Wait a bit more for schema evolution to happen
        Thread.sleep(2000);

        // Verify the new column exists in target
        boolean columnExists = testUtils().targetColumnExists("phone_number");
        assertThat(columnExists).isTrue();

        LOGGER.info("Schema evolution test passed - column 'phone_number' added to target");
    }

    @Test
    @Order(6)
    public void testMultipleOperations() throws Exception {
        Testing.Print.enable();

        LOGGER.info("Testing multiple operations in sequence...");

        int initialCount = testUtils().getTargetRecordCount();

        // Perform multiple operations
        testUtils().insertSourceRecord(4001, "Alice", "Johnson", "alice@example.com");
        testUtils().insertSourceRecord(4002, "Bob", "Williams", "bob@example.com");
        testUtils().updateSourceRecord(4001, "Alice Updated", "Johnson Updated", "alice.updated@example.com");
        // Delete customer 1004 (Anne Kretchmar) - less likely to have orders
        testUtils().deleteSourceRecord(1004);

        // Wait for all operations to complete
        // Expected: initial + 2 inserts - 1 delete
        int expectedCount = initialCount + 2 - 1;

        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    try {
                        int count = testUtils().getTargetRecordCount();
                        LOGGER.debug("Target has {} records (expecting {})", count, expectedCount);
                        return count == expectedCount;
                    }
                    catch (Exception e) {
                        return false;
                    }
                });

        // Verify final state
        assertThat(testUtils().getTargetRecordCount()).isEqualTo(expectedCount);

        // Verify the updated record
        Map<String, Object> alice = testUtils().getTargetRecordById(4001);
        assertThat(alice).isNotNull();
        assertThat(alice.get("first_name")).isEqualTo("Alice Updated");

        // Verify the inserted record
        Map<String, Object> bob = testUtils().getTargetRecordById(4002);
        assertThat(bob).isNotNull();
        assertThat(bob.get("first_name")).isEqualTo("Bob");

        // Verify the deleted record is gone
        Map<String, Object> deleted = testUtils().getTargetRecordById(1004);
        assertThat(deleted).isNull();

        LOGGER.info("Multiple operations test passed");
    }
}
