/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import static io.debezium.server.redis.TestConstants.INITIAL_CUSTOMER_COUNT;
import static io.debezium.server.redis.TestConstants.INITIAL_SCHEMA_HISTORY_SIZE;
import static io.debezium.server.redis.TestUtils.awaitHashSizeGte;
import static io.debezium.server.redis.TestUtils.getMySqlConnection;
import static io.debezium.server.redis.TestUtils.insertCustomerToMySql;
import static io.debezium.server.redis.TestUtils.waitForStreamLength;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.mysql.strategy.mysql.MySqlConnection;
import io.debezium.doc.FixFor;

import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;

public class TestContainersStreamIT extends TestContainersRedisTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestContainersStreamIT.class);

    @Test
    public void shouldStreamChanges() throws InterruptedException, IOException {
        startServerWithEnv(new DebeziumServerConfigBuilder().withBaseMySqlRedisConfig(redis, mysql).build());

        final String STREAM_NAME = "testc.inventory.customers";

        waitForStreamLength(jedis, STREAM_NAME, INITIAL_CUSTOMER_COUNT);
        assertThat(server.getStandardOutput()).contains("inventory.customers");

        insertCustomerToMySql(mysql, "Sergei", "Savage", "sesa@email.com");

        waitForStreamLength(jedis, STREAM_NAME, INITIAL_CUSTOMER_COUNT + 1);
    }

    @Test
    public void shouldFailWithIncorrectRedisAddress() {
        startServerWithEnv(new DebeziumServerConfigBuilder()
                .withBaseMySqlRedisConfig(redis, mysql)
                .withValue("debezium.sink.redis.address", redis.getContainerAddress() + ":" + 1000)
                .build());

        server.waitForContainerLog("Failed to connect to any host resolved for DNS name");
        server.waitForStop();
    }

    @Test
    @FixFor("DBZ-4510")
    public void shouldRetryAfterRedisCrash() throws Exception {
        final int SOCKET_TIMEOUT = 4000;
        startServerWithEnv(new DebeziumServerConfigBuilder().withBaseMySqlRedisConfig(redis, mysql)
                .withValue("debezium.sink.redis.socket.timeout.ms", String.valueOf(SOCKET_TIMEOUT))
                .build());

        final String STREAM_NAME = "testc.inventory.customers";
        waitForStreamLength(jedis, STREAM_NAME, INITIAL_CUSTOMER_COUNT);
        redis.pause();

        insertCustomerToMySql(mysql, "Sergei", "Savage", "sesa@email.com");

        LOGGER.info("Sleeping for " + SOCKET_TIMEOUT / 2 + " milis to simulate no connection errors");
        Thread.sleep(SOCKET_TIMEOUT / 2);
        redis.unpause();
        waitForStreamLength(jedis, STREAM_NAME, INITIAL_CUSTOMER_COUNT + 1);
    }

    @Test
    public void shouldTimeoutAfterRedisCrash() throws Exception {
        final int SOCKET_TIMEOUT = 2000;
        startServerWithEnv(new DebeziumServerConfigBuilder().withBaseMySqlRedisConfig(redis, mysql)
                .withValue("debezium.sink.redis.socket.timeout.ms", String.valueOf(SOCKET_TIMEOUT))
                .build());

        final String STREAM_NAME = "testc.inventory.customers";
        waitForStreamLength(jedis, STREAM_NAME, INITIAL_CUSTOMER_COUNT);

        redis.pause();
        insertCustomerToMySql(mysql, "Sergei", "Savage", "sesa@email.com");

        LOGGER.info("Sleeping for " + SOCKET_TIMEOUT / 2 + " milis to simulate no connection errors");
        Thread.sleep(SOCKET_TIMEOUT / 2);
        assertThat(server.isRunning()).isTrue();

        server.waitForContainerLog("Read timed out");
        server.waitForStop();
    }

    @Test
    @FixFor("DBZ-4510")
    public void shouldRetryAfterRedisOOM() throws Exception {
        startServerWithEnv(new DebeziumServerConfigBuilder().withBaseMySqlRedisConfig(redis, mysql).build());

        final String STREAM_NAME = "testc.inventory.customers";
        final int INSERTED_RECORDS_COUNT = 1000;
        final int STARTING_ID = 1010;

        LOGGER.info("Setting Redis' maxmemory to 2M");
        jedis.configSet("maxmemory", "2M");

        MySqlConnection connection = getMySqlConnection(mysql);
        connection.execute("CREATE PROCEDURE FillRedisTest()\n" +
                "BEGIN\n" +
                "DECLARE startingRange INT DEFAULT " + STARTING_ID + ";\n" +
                "WHILE startingRange < " + (STARTING_ID + INSERTED_RECORDS_COUNT) + " DO\n" +
                "  INSERT INTO inventory.customers (id,first_name,last_name, email) VALUES (startingRange, (SELECT LEFT(UUID(), 8)), (SELECT LEFT(UUID(), 8)), CONCAT((SELECT LEFT(UUID(), 8)), \"@test.com\"));\n"
                +
                "  SET startingRange = startingRange + 1;\n" +
                "END WHILE;\n" +
                "END ;\n");
        connection.execute("CALL FillRedisTest;");
        connection.commit();

        server.waitForContainerLog("Sink memory threshold percentage was reached");
        assertThat(jedis.xlen(STREAM_NAME)).isLessThan(INSERTED_RECORDS_COUNT);

        LOGGER.info("Unsetting Redis' maxmemory");
        jedis.configSet("maxmemory", "30M");
        waitForStreamLength(jedis, STREAM_NAME, INSERTED_RECORDS_COUNT + INITIAL_CUSTOMER_COUNT);
    }

    @Test
    public void shouldStreamExtendedMessageFormat() {
        startServerWithEnv(new DebeziumServerConfigBuilder().withBaseMySqlRedisConfig(redis, mysql)
                .withValue("debezium.sink.redis.message.format", "extended")
                .build());
        final String STREAM_NAME = "testc.inventory.customers";

        waitForStreamLength(jedis, STREAM_NAME, INITIAL_CUSTOMER_COUNT);

        final List<StreamEntry> entries = jedis.xrange(STREAM_NAME, null, (StreamEntryID) null);
        for (StreamEntry entry : entries) {
            Map<String, String> map = entry.getFields();
            // TODO verify, that there should really be 2 fields
            // assertEquals(3, map.size(), "Expected map of size 3");
            assertThat(map.get("key")).startsWith("{\"schema\":");
            assertThat(map.get("value")).startsWith("{\"schema\":");
        }
    }

    @Test
    @FixFor("DBZ-4509")
    public void shouldStreamSchemaHistory() throws Exception {
        final String STREAM_NAME = "metadata:debezium:schema_history";
        final String TABLE_NAME = "redis_test";
        startServerWithEnv(new DebeziumServerConfigBuilder()
                .withBaseMySqlRedisConfig(redis, mysql)
                .withValue("debezium.source.schema.history.internal", "io.debezium.server.redis.RedisSchemaHistory")
                .build());

        waitForStreamLength(jedis, STREAM_NAME, INITIAL_SCHEMA_HISTORY_SIZE);
        redis.pause();

        final MySqlConnection connection = getMySqlConnection(mysql);
        LOGGER.info("Creating new " + TABLE_NAME + " table");
        connection.execute("CREATE TABLE inventory." + TABLE_NAME + " (id INT PRIMARY KEY)");
        connection.close();

        redis.unpause();
        waitForStreamLength(jedis, STREAM_NAME, INITIAL_SCHEMA_HISTORY_SIZE + 1);

        final List<StreamEntry> entries = jedis.xrange(STREAM_NAME, null, (StreamEntryID) null);
        assertTrue(entries.get(INITIAL_SCHEMA_HISTORY_SIZE).getFields().get("schema").contains(TABLE_NAME));
    }

    /**
     * Test retry mechanism when encountering Redis connectivity issues:
     * 1. Make Redis to be unavailable while the server is up
     * 2. Create a new table named redis_test in PostgreSQL and insert 5 records to it
     * 3. Bring Redis up again and make sure the offsets have been written successfully
     */
    @Test
    @FixFor("DBZ-4509")
    public void shouldStoreOffsetInRedis() throws Exception {
        final String OFFSETS_HASH_NAME = "metadata:debezium:offsets";
        startServerWithEnv(new DebeziumServerConfigBuilder()
                .withBaseMySqlRedisConfig(redis, mysql)
                .withValue("debezium.source.offset.storage", "io.debezium.server.redis.RedisOffsetBackingStore")
                .build());

        awaitHashSizeGte(jedis, OFFSETS_HASH_NAME, 1);
        jedis.del(OFFSETS_HASH_NAME);
        redis.pause();

        final MySqlConnection connection = getMySqlConnection(mysql);
        LOGGER.info("Creating new redis_test table and inserting 5 records to it");
        connection.execute(
                "CREATE TABLE inventory.redis_test (id INT PRIMARY KEY)",
                "INSERT INTO inventory.redis_test VALUES (1)",
                "INSERT INTO inventory.redis_test VALUES (2)",
                "INSERT INTO inventory.redis_test VALUES (3)",
                "INSERT INTO inventory.redis_test VALUES (4)",
                "INSERT INTO inventory.redis_test VALUES (5)");
        connection.close();
        server.waitForContainerLog("Connection to Redis failed");

        redis.unpause();
        // wait until the offsets are re-written
        awaitHashSizeGte(jedis, OFFSETS_HASH_NAME, 1);
    }
}
