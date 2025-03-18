/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import static io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager.JDBC_POSTGRESQL_URL_FORMAT;
import static io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager.POSTGRES_HOST;
import static io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager.POSTGRES_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import jakarta.inject.Inject;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.testing.system.tools.databases.SqlDatabaseClient;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test that verifies basic reading from PostgreSQL database.
 *
 * @author Fiore Mario Vitale
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DebeziumServerPostgresIT {

    private static final int MESSAGE_COUNT = 4;
    @Inject
    DebeziumServer server;

    @Inject
    DebeziumMetrics metrics;

    @BeforeEach
    void setUp() {
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(TestConfigSource.OFFSET_STORE_PATH);
    }

    @Test
    @Order(1)
    public void shouldSnapshot() {

        Testing.Print.enable();

        final TestConsumer testConsumer = (TestConsumer) server.getConsumer();

        waitSnapshotCompletion();

        assertThat(testConsumer.getValues().size()).isEqualTo(MESSAGE_COUNT);

        List<String> values = testConsumer.getValues().stream().map(Object::toString).collect(Collectors.toList());

        assertThat(values.get(0)).contains("\"after\":{\"id\":1001,\"first_name\":\"Sally\",\"last_name\":\"Thomas\",\"email\":\"sally.thomas@acme.com\"}");
        assertThat(values.get(1)).contains("\"after\":{\"id\":1002,\"first_name\":\"George\",\"last_name\":\"Bailey\",\"email\":\"gbailey@foobar.com\"}");
        assertThat(values.get(2)).contains("\"after\":{\"id\":1003,\"first_name\":\"Edward\",\"last_name\":\"Walker\",\"email\":\"ed@walker.com\"}");
        assertThat(values.get(3)).contains("\"after\":{\"id\":1004,\"first_name\":\"Anne\",\"last_name\":\"Kretchmar\",\"email\":\"annek@noanswer.org\"}");
    }

    @Test
    @Order(2)
    public void shouldStream() throws SQLException {
        Testing.Print.enable();

        final TestConsumer testConsumer = (TestConsumer) server.getConsumer();

        waitSnapshotCompletion();

        insertNewRow();

        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds()))
                .until(() -> (testConsumer.getValues().size() >= MESSAGE_COUNT + 1));

        List<String> values = testConsumer.getValues().stream().map(Object::toString).collect(Collectors.toList());

        assertThat(values.get(0)).contains("\"after\":{\"id\":1001,\"first_name\":\"Sally\",\"last_name\":\"Thomas\",\"email\":\"sally.thomas@acme.com\"}");
        assertThat(values.get(1)).contains("\"after\":{\"id\":1002,\"first_name\":\"George\",\"last_name\":\"Bailey\",\"email\":\"gbailey@foobar.com\"}");
        assertThat(values.get(2)).contains("\"after\":{\"id\":1003,\"first_name\":\"Edward\",\"last_name\":\"Walker\",\"email\":\"ed@walker.com\"}");
        assertThat(values.get(3)).contains("\"after\":{\"id\":1004,\"first_name\":\"Anne\",\"last_name\":\"Kretchmar\",\"email\":\"annek@noanswer.org\"}");
        assertThat(values.get(4)).contains("\"after\":{\"id\":1005,\"first_name\":\"Jon\",\"last_name\":\"Snow\",\"email\":\"jon_snow@gameofthrones.com\"}");

    }

    private void waitSnapshotCompletion() {
        Awaitility.await().atMost(Duration.ofSeconds(TestConfigSource.waitForSeconds())).until(() -> {
            try {
                // snapshot process finished
                // and consuming events finished!
                return metrics.snapshotCompleted()
                        && metrics.streamingQueueCurrentSize() == 0
                        && metrics.maxQueueSize() == CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE;
            }
            catch (Exception e) {
                return false;
            }
        });
    }

    private static void insertNewRow() throws SQLException {

        SqlDatabaseClient sqlDatabaseClient = new SqlDatabaseClient(getJdbcUrl(),
                PostgresTestResourceLifecycleManager.POSTGRES_USER,
                PostgresTestResourceLifecycleManager.POSTGRES_PASSWORD);

        String sql = "INSERT INTO inventory.customers VALUES  (default, 'Jon', 'Snow', 'jon_snow@gameofthrones.com')";
        sqlDatabaseClient.execute("inventory", sql);
    }

    public static String getJdbcUrl() {

        return String.format(JDBC_POSTGRESQL_URL_FORMAT, POSTGRES_HOST, PostgresTestResourceLifecycleManager.getContainer().getMappedPort(POSTGRES_PORT).toString());
    }
}
