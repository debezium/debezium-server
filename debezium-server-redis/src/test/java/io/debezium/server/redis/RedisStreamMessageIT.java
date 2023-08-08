/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;

/**
 * Integration tests that verify basic reading from PostgreSQL database and writing to Redis stream
 *
 * @author ggaborg
 */
@Disabled
@QuarkusIntegrationTest
@TestProfile(RedisStreamMessageTestProfile.class)
@QuarkusTestResource(RedisTestResourceLifecycleManager.class)
public class RedisStreamMessageIT {

    /**
    *  Verifies that all the records of a PostgreSQL table are streamed to Redis in extended message format
    */
    @Test
    public void testRedisStreamExtendedMessage() {
        Testing.Print.enable();

        Jedis jedis = new Jedis(HostAndPort.from(RedisTestResourceLifecycleManager.getRedisContainerAddress()));
        final int MESSAGE_COUNT = 4;
        final String STREAM_NAME = "testc.inventory.customers";

        TestUtils.awaitStreamLengthGte(jedis, STREAM_NAME, MESSAGE_COUNT);

        long streamLength = jedis.xlen(STREAM_NAME);
        assertEquals(MESSAGE_COUNT, streamLength, "Expected stream length of " + MESSAGE_COUNT);

        final List<StreamEntry> entries = jedis.xrange(STREAM_NAME, (StreamEntryID) null, (StreamEntryID) null);
        for (StreamEntry entry : entries) {
            Map<String, String> map = entry.getFields();
            assertEquals(3, map.size(), "Expected map of size 3");
            assertTrue(map.get("key") != null && map.get("key").startsWith("{\"schema\":"), "Expected key's value starting with {\"schema\":...");
            assertTrue(map.get("value") != null && map.get("value").startsWith("{\"schema\":"), "Expected value's value starting with {\"schema\":...");
            assertTrue(map.containsKey("HEADERKEY"));
        }

        jedis.close();
    }

}
