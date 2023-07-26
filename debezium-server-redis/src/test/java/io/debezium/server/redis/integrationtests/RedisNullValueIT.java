package io.debezium.server.redis.integrationtests;

import io.debezium.server.redis.TestUtils;
import io.debezium.server.redis.lifecyclemanagers.RedisTestResourceLifecycleManager;
import io.debezium.server.redis.profiles.RedisNullValueTestProfile;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static io.debezium.server.redis.profiles.RedisNullValueTestProfile.NULL_VALUE_REPLACEMENT;
import static org.assertj.core.api.Assertions.assertThat;

@QuarkusIntegrationTest
@TestProfile(RedisNullValueTestProfile.class)
public class RedisNullValueIT {

    @Test
    public void nullValueTest() throws IOException, InterruptedException {
        Jedis jedis = new Jedis(HostAndPort.from(RedisTestResourceLifecycleManager.getRedisContainerAddress()));
        final String STREAM_NAME = "testc.inventory.customers";
        final int BEFORE_MESSAGE_COUNT = 4;
        final int AFTER_MESSAGE_COUNT = 6;

        TestUtils.awaitStreamLengthGte(jedis, STREAM_NAME, BEFORE_MESSAGE_COUNT);

        PostgresTestResourceLifecycleManager.getContainer().execInContainer("psql",
                "-U", PostgresTestResourceLifecycleManager.POSTGRES_USER,
                "-d", PostgresTestResourceLifecycleManager.POSTGRES_DBNAME,
                "-c", "DELETE FROM inventory.customers WHERE ID = 1004");

        TestUtils.awaitStreamLengthGte(jedis, STREAM_NAME, AFTER_MESSAGE_COUNT);

        // assert there is one field, that had null value replaced
        final List<StreamEntry> entries = jedis.xrange(STREAM_NAME, null, (StreamEntryID) null);
        var emptyFields = entries
                .stream()
                .filter(e -> NULL_VALUE_REPLACEMENT.equals(e.getFields().entrySet().iterator().next().getValue()))
                .collect(Collectors.toList());
        assertThat(emptyFields.size()).isEqualTo(1);

        jedis.close();
    }
}
