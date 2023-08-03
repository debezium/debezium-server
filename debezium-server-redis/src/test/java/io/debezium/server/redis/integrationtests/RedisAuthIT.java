package io.debezium.server.redis.integrationtests;

import org.junit.jupiter.api.Test;

import io.debezium.server.redis.TestUtils;
import io.debezium.server.redis.lifecyclemanagers.RedisAuthTestLifecycleManager;
import io.debezium.server.redis.profiles.RedisAuthTestProfile;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisClientConfig;

@QuarkusIntegrationTest
@TestProfile(RedisAuthTestProfile.class)
public class RedisAuthIT {
    private final JedisClientConfig config = new JedisClientConfig() {
        @Override
        public String getUser() {
            return "debezium";
        }

        @Override
        public String getPassword() {
            return "dbz";
        }
    };

    @Test
    public void passwordAuthTest() {
        Jedis jedis = new Jedis(HostAndPort.from(RedisAuthTestLifecycleManager.getRedisContainerAddress()), config);
        final int MESSAGE_COUNT = 4;
        final String STREAM_NAME = "testc.inventory.customers";

        TestUtils.awaitStreamLengthGte(jedis, STREAM_NAME, MESSAGE_COUNT);
    }
}
