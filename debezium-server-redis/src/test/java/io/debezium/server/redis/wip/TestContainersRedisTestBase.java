/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis.wip;

import static io.debezium.server.redis.wip.TestConstants.POSTGRES_DATABASE;
import static io.debezium.server.redis.wip.TestConstants.POSTGRES_IMAGE;
import static io.debezium.server.redis.wip.TestConstants.POSTGRES_PASSWORD;
import static io.debezium.server.redis.wip.TestConstants.POSTGRES_PORT;
import static io.debezium.server.redis.wip.TestConstants.POSTGRES_USER;
import static io.debezium.server.redis.wip.TestConstants.REDIS_IMAGE;
import static io.debezium.server.redis.wip.TestConstants.REDIS_PORT;
import static io.debezium.server.redis.wip.TestProperties.DEBEZIUM_SERVER_IMAGE;
import static io.debezium.server.redis.wip.TestUtils.getRedisContainerAddress;

import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

public class TestContainersRedisTestBase {
    protected DebeziumTestContainerWrapper postgres;
    protected DebeziumTestContainerWrapper redis;
    protected DebeziumTestContainerWrapper server;
    protected Jedis jedis;

    public TestContainersRedisTestBase() {
        // provide base configuration for all components
        postgres = new DebeziumTestContainerWrapper(POSTGRES_IMAGE)
                .withExposedPorts(POSTGRES_PORT)
                .withEnv(Map.of("POSTGRES_USER", POSTGRES_USER,
                        "POSTGRES_PASSWORD", POSTGRES_PASSWORD,
                        "POSTGRES_DB", POSTGRES_DATABASE,
                        "POSTGRES_INITDB_ARGS", "\"-E UTF8\"",
                        "LANG", "en_US.utf8"));
        redis = new DebeziumTestContainerWrapper(REDIS_IMAGE)
                .withExposedPorts(REDIS_PORT);
        server = new DebeziumTestContainerWrapper(DEBEZIUM_SERVER_IMAGE);
    }

    @BeforeEach
    public void setUp() {
        postgres.start();
        redis.start();
        jedis = new Jedis(HostAndPort.from(getRedisContainerAddress(redis)));

    }

    @AfterEach
    public void tearDown() {
        server.stop();
        postgres.stop();
        redis.stop();
    }

}
