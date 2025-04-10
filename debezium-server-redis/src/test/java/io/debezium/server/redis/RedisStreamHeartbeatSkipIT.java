/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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

        // Wait for some time to allow heartbeats to be generated
        Testing.print("Waiting for heartbeats to be generated...");
        Thread.sleep(3000);

        // Check for all streams in Redis
        Set<String> allKeys = jedis.keys("*");
        List<String> streams = allKeys.stream()
                .filter(key -> {
                    try {
                        // Check if the key is a stream by attempting to get its length
                        jedis.xlen(key);
                        return true;
                    }
                    catch (Exception e) {
                        return false;
                    }
                })
                .collect(Collectors.toList());

        // Look for heartbeat streams
        List<String> heartbeatStreams = streams.stream()
                .filter(stream -> stream.contains("__debezium-heartbeat"))
                .collect(Collectors.toList());

        Testing.print("All streams: " + streams);
        Testing.print("Heartbeat streams found: " + heartbeatStreams);

        // Verify that we have data streams but no heartbeat streams
        assertTrue("Expected to find at least one data stream",
                streams.stream().anyMatch(s -> s.startsWith("testc.inventory.")));

        assertEquals("Expected no heartbeat streams with skip.heartbeat.messages=true",
                0, heartbeatStreams.size());

        jedis.close();
    }
}
