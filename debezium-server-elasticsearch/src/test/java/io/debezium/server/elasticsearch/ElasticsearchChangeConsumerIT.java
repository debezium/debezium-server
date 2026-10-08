/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;

import jakarta.enterprise.event.Observes;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.runtime.events.ConnectorStartedEvent;
import io.debezium.runtime.events.DebeziumCompletionEvent;
import io.debezium.server.TestConfigSource;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch.core.GetResponse;

/**
 * Integration tests for the Elasticsearch sink with a PostgreSQL source.
 * <p>
 * Tests cover:
 * - Initial snapshot replication
 * - INSERT operations
 * - UPDATE operations (upsert write method)
 * - DELETE operations
 * - Schema evolution
 *
 * @author Chris Cranford
 */
@QuarkusTest
@TestProfile(ElasticsearchTestProfile.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ElasticsearchChangeConsumerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchChangeConsumerIT.class);

    private static final String CUSTOMERS_INDEX = "testserver.inventory.customers";
    private static final String PRODUCTS_INDEX = "testserver.inventory.products";
    private static final int INITIAL_CUSTOMER_COUNT = 4;
    private static final int INITIAL_PRODUCT_COUNT = 9;

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

        awaitDocumentCount(CUSTOMERS_INDEX, INITIAL_CUSTOMER_COUNT, Duration.ofSeconds(120));
        awaitDocumentCount(PRODUCTS_INDEX, INITIAL_PRODUCT_COUNT, Duration.ofSeconds(60));

        // Verify the first customer (Sally Thomas from the example-postgres image)
        final Map<String, Object> sally = getDocument(CUSTOMERS_INDEX, "1001");
        assertThat(sally).isNotNull();
        assertThat(sally.get("id")).isEqualTo(1001);
        assertThat(sally.get("first_name")).isEqualTo("Sally");
        assertThat(sally.get("last_name")).isEqualTo("Thomas");
        assertThat(sally.get("email")).isEqualTo("sally.thomas@acme.com");

        LOGGER.info("Initial snapshot test passed");
    }

    @Test
    @Order(2)
    public void testInsertOperation() throws Exception {
        Testing.Print.enable();

        LOGGER.info("Inserting new record into source database...");

        insertSourceCustomer(2001, "John", "Doe", "john.doe@example.com");

        awaitDocument(CUSTOMERS_INDEX, "2001", Duration.ofSeconds(30));

        final Map<String, Object> john = getDocument(CUSTOMERS_INDEX, "2001");
        assertThat(john).isNotNull();
        assertThat(john.get("first_name")).isEqualTo("John");
        assertThat(john.get("last_name")).isEqualTo("Doe");
        assertThat(john.get("email")).isEqualTo("john.doe@example.com");

        LOGGER.info("Insert operation test passed");
    }

    @Test
    @Order(3)
    public void testUpdateOperation() throws Exception {
        Testing.Print.enable();

        LOGGER.info("Updating existing record in source database...");

        updateSourceCustomer(1001, "Sally Updated", "Thomas Updated", "sally.updated@acme.com");

        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    final Map<String, Object> document = getDocument(CUSTOMERS_INDEX, "1001");
                    return document != null && "Sally Updated".equals(document.get("first_name"));
                });

        final Map<String, Object> sally = getDocument(CUSTOMERS_INDEX, "1001");
        assertThat(sally).isNotNull();
        assertThat(sally.get("first_name")).isEqualTo("Sally Updated");
        assertThat(sally.get("last_name")).isEqualTo("Thomas Updated");
        assertThat(sally.get("email")).isEqualTo("sally.updated@acme.com");

        LOGGER.info("Update operation test passed");
    }

    @Test
    @Order(4)
    public void testDeleteOperation() throws Exception {
        Testing.Print.enable();

        LOGGER.info("Deleting product from source database...");

        assertThat(documentExists(PRODUCTS_INDEX, "101")).isTrue();

        deleteSourceProduct(101);

        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> !documentExists(PRODUCTS_INDEX, "101"));

        assertThat(documentExists(PRODUCTS_INDEX, "101")).isFalse();
        assertThat(getDocumentCount(PRODUCTS_INDEX)).isEqualTo(INITIAL_PRODUCT_COUNT - 1);

        LOGGER.info("Delete operation test passed");
    }

    @Test
    @Order(5)
    public void testSchemaEvolution() throws Exception {
        Testing.Print.enable();

        LOGGER.info("Adding new column to source table...");

        executeSourceSql("ALTER TABLE inventory.customers ADD COLUMN phone_number VARCHAR(50)");
        executeSourceSql("INSERT INTO inventory.customers (id, first_name, last_name, email, phone_number) "
                + "VALUES (3001, 'Jane', 'Smith', 'jane.smith@example.com', '555-0100')");

        awaitDocument(CUSTOMERS_INDEX, "3001", Duration.ofSeconds(30));

        final Map<String, Object> jane = getDocument(CUSTOMERS_INDEX, "3001");
        assertThat(jane).isNotNull();
        assertThat(jane.get("phone_number")).isEqualTo("555-0100");

        // The connector regenerates the mapping when the value schema changes
        final Map<String, Property> properties = getClient().indices()
                .getMapping(m -> m.index(CUSTOMERS_INDEX))
                .get(CUSTOMERS_INDEX)
                .mappings()
                .properties();
        assertThat(properties).containsKey("phone_number");

        LOGGER.info("Schema evolution test passed - field 'phone_number' mapped in target");
    }

    @Test
    @Order(6)
    public void testMultipleOperations() throws Exception {
        Testing.Print.enable();

        LOGGER.info("Testing multiple operations in sequence...");

        final long initialCount = getDocumentCount(CUSTOMERS_INDEX);

        insertSourceCustomer(4001, "Alice", "Johnson", "alice@example.com");
        insertSourceCustomer(4002, "Bob", "Williams", "bob@example.com");
        updateSourceCustomer(4001, "Alice Updated", "Johnson Updated", "alice.updated@example.com");
        // Delete customer 1004 (Anne Kretchmar), which has no dependent rows in the example data
        executeSourceSql("DELETE FROM inventory.customers WHERE id = 1004");

        // Expected: initial + 2 inserts - 1 delete
        final long expectedCount = initialCount + 2 - 1;

        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> getDocumentCount(CUSTOMERS_INDEX) == expectedCount && !documentExists(CUSTOMERS_INDEX, "1004"));

        assertThat(getDocumentCount(CUSTOMERS_INDEX)).isEqualTo(expectedCount);

        final Map<String, Object> alice = getDocument(CUSTOMERS_INDEX, "4001");
        assertThat(alice).isNotNull();
        assertThat(alice.get("first_name")).isEqualTo("Alice Updated");

        final Map<String, Object> bob = getDocument(CUSTOMERS_INDEX, "4002");
        assertThat(bob).isNotNull();
        assertThat(bob.get("first_name")).isEqualTo("Bob");

        assertThat(documentExists(CUSTOMERS_INDEX, "1004")).isFalse();

        LOGGER.info("Multiple operations test passed");
    }

    private static ElasticsearchClient getClient() {
        return ElasticsearchTestResourceLifecycleManager.getClient();
    }

    private static void awaitDocumentCount(String index, long expected, Duration timeout) {
        Awaitility.await()
                .atMost(timeout)
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    try {
                        final long count = getDocumentCount(index);
                        LOGGER.debug("Index {} has {} documents (expecting {})", index, count, expected);
                        return count >= expected;
                    }
                    catch (Exception e) {
                        LOGGER.debug("Error counting documents in {}: {}", index, e.getMessage());
                        return false;
                    }
                });
    }

    private static void awaitDocument(String index, String id, Duration timeout) {
        Awaitility.await()
                .atMost(timeout)
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> documentExists(index, id));
    }

    private static long getDocumentCount(String index) throws IOException {
        if (!getClient().indices().exists(e -> e.index(index)).value()) {
            return 0;
        }
        getClient().indices().refresh(r -> r.index(index));
        return getClient().count(c -> c.index(index)).count();
    }

    private static boolean documentExists(String index, String id) throws IOException {
        if (!getClient().indices().exists(e -> e.index(index)).value()) {
            return false;
        }
        return getClient().exists(e -> e.index(index).id(id)).value();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getDocument(String index, String id) throws IOException {
        if (!getClient().indices().exists(e -> e.index(index)).value()) {
            return null;
        }
        final GetResponse<Map> response = getClient().get(g -> g.index(index).id(id), Map.class);
        return response.found() ? (Map<String, Object>) response.source() : null;
    }

    private static Connection getSourceConnection() throws SQLException {
        final String url = String.format(PostgresTestResourceLifecycleManager.JDBC_POSTGRESQL_URL_FORMAT,
                PostgresTestResourceLifecycleManager.POSTGRES_HOST,
                PostgresTestResourceLifecycleManager.getContainer().getMappedPort(PostgresTestResourceLifecycleManager.POSTGRES_PORT))
                + PostgresTestResourceLifecycleManager.POSTGRES_DBNAME;
        return DriverManager.getConnection(url,
                PostgresTestResourceLifecycleManager.POSTGRES_USER,
                PostgresTestResourceLifecycleManager.POSTGRES_PASSWORD);
    }

    private static void executeSourceSql(String... statements) throws SQLException {
        try (Connection connection = getSourceConnection(); Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }

    private static void insertSourceCustomer(int id, String firstName, String lastName, String email) throws SQLException {
        executeSourceSql(String.format(
                "INSERT INTO inventory.customers (id, first_name, last_name, email) VALUES (%d, '%s', '%s', '%s')",
                id, firstName, lastName, email));
    }

    private static void updateSourceCustomer(int id, String firstName, String lastName, String email) throws SQLException {
        executeSourceSql(String.format(
                "UPDATE inventory.customers SET first_name = '%s', last_name = '%s', email = '%s' WHERE id = %d",
                firstName, lastName, email, id));
    }

    private static void deleteSourceProduct(int id) throws SQLException {
        // products_on_hand references products; remove the dependent row first
        executeSourceSql(
                String.format("DELETE FROM inventory.products_on_hand WHERE product_id = %d", id),
                String.format("DELETE FROM inventory.products WHERE id = %d", id));
    }
}
