/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
 * Integration test that verifies heartbeat messages are properly skipped in Redis Stream
 *
 * @author Yossi Shirizli
 */
@QuarkusIntegrationTest
@TestProfile(RedisStreamHeartbeatTestProfile.class)
@QuarkusTestResource(RedisTestResourceLifecycleManager.class)
public class RedisStreamHeartbeatSkipIT {

    private static final String HEARTBEAT_PREFIX = "__debezium-heartbeat";

    /**
     * Test that heartbeat messages are skipped when skip.heartbeat.messages=true
     */
    @Test
    public void testHeartbeatMessagesSkipped() throws Exception {
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

        // Wait for data streams to appear
        Testing.print("Waiting for data streams to be generated...");

        Awaitility.await()
                .atMost(3, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .until(() -> {
                    // Direct search for data streams
                    List<String> dataStreams = jedis.keys("testc.inventory.*").stream()
                            .filter(key -> {
                                try {
                                    // Check if it's a stream and has entries
                                    return jedis.xlen(key) > 0;
                                }
                                catch (Exception e) {
                                    return false;
                                }
                            })
                            .collect(Collectors.toList());

                    return !dataStreams.isEmpty();
                });

        // Check directly for data streams
        List<String> dataStreams = jedis.keys("testc.inventory.*").stream()
                .filter(key -> {
                    try {
                        // Check if it's a stream and has entries
                        return jedis.xlen(key) > 0;
                    }
                    catch (Exception e) {
                        return false;
                    }
                })
                .collect(Collectors.toList());

        // Look directly for heartbeat streams using the pattern
        List<String> heartbeatStreams = jedis.keys(HEARTBEAT_PREFIX + "*").stream()
                .filter(key -> {
                    try {
                        // Check if it's a stream
                        jedis.xlen(key);
                        return true;
                    }
                    catch (Exception e) {
                        return false;
                    }
                })
                .collect(Collectors.toList());

        Testing.print("Data streams found: " + dataStreams);
        Testing.print("Heartbeat streams found: " + heartbeatStreams);

        // Verify that we have data streams but no heartbeat streams
        assertFalse(dataStreams.isEmpty(), "Expected to find at least one data stream");
        assertEquals(0, heartbeatStreams.size(),
                "Expected no heartbeat streams with skip.heartbeat.messages=true");

        jedis.close();
    }
}
