/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

/**
 * Integration test that verifies heartbeat messages are stored in Redis Stream when skip.heartbeat.messages=false
 *
 * @author Yossi Shirizli
 */
@QuarkusIntegrationTest
@TestProfile(RedisStreamHeartbeatDisabledTestProfile.class)
@QuarkusTestResource(RedisTestResourceLifecycleManager.class)
public class RedisStreamHeartbeatDisabledIT {

    private static final String HEARTBEAT_PREFIX = "__debezium-heartbeat";

    /**
     * Test that heartbeat messages are stored when skip.heartbeat.messages=false
     */
    @Test
    public void testHeartbeatMessagesStored() throws Exception {
        Testing.Print.enable();

        // Get a connection to Redis
        Jedis jedis = new Jedis(HostAndPort.from(RedisTestResourceLifecycleManager.getRedisContainerAddress()));

        // Create a table and insert data to trigger the CDC process
        final PostgresConnection connection = TestUtils.getPostgresConnection();
        Testing.print("Creating new redis_test table and inserting 2 records to it");
        connection.execute(
                "CREATE TABLE inventory.redis_test (id INT PRIMARY KEY)",
                "INSERT INTO inventory.redis_test VALUES (1)",
                "INSERT INTO inventory.redis_test VALUES (2)");
        connection.close();

        // Wait specifically for heartbeat streams to appear and have entries
        Testing.print("Waiting for heartbeats to be generated...");

        Awaitility.await()
                .atMost(3, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .until(() -> {
                    List<String> heartbeatStreams = jedis.keys(HEARTBEAT_PREFIX + "*").stream()
                            .filter(key -> {
                                try {
                                    // Check if it's a stream and has entries
                                    return jedis.xlen(key) > 0;
                                }
                                catch (Exception e) {
                                    return false;
                                }
                            })
                            .toList();

                    return !heartbeatStreams.isEmpty();
                });

        // After successful waiting, verify data streams
        List<String> dataStreams = jedis.keys("testc.inventory.*").stream()
                .filter(key -> {
                    try {
                        return jedis.xlen(key) > 0;
                    }
                    catch (Exception e) {
                        return false;
                    }
                })
                .toList();

        Testing.print("Data streams found: " + dataStreams);

        // Verify that we have data streams
        assertFalse(dataStreams.isEmpty(), "Expected to find at least one data stream");

        jedis.close();
    }
}
