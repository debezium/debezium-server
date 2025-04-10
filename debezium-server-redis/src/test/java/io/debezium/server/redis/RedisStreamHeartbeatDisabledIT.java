/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

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
 * Integration test that verifies heartbeat messages are stored in Redis Stream when skip.heartbeat.messages=false
 *
 * @author Yossi Shirizli
 */
@QuarkusIntegrationTest
@TestProfile(RedisStreamHeartbeatDisabledTestProfile.class)
@QuarkusTestResource(RedisTestResourceLifecycleManager.class)
public class RedisStreamHeartbeatDisabledIT {

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

        // Verify that we have both data streams and heartbeat streams
        assertTrue("Expected to find at least one data stream",
                streams.stream().anyMatch(s -> s.startsWith("testc.inventory.")));

        assertTrue("Expected to find at least one heartbeat stream with skip.heartbeat.messages=false",
                !heartbeatStreams.isEmpty());

        // Verify that the heartbeat streams have entries
        for (String heartbeatStream : heartbeatStreams) {
            long streamLength = jedis.xlen(heartbeatStream);
            Testing.print("Heartbeat stream " + heartbeatStream + " has " + streamLength + " entries");
            assertTrue("Expected heartbeat stream to have entries", streamLength > 0);
        }

        jedis.close();
    }
}
