/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis.wip;

import static io.debezium.server.redis.wip.TestConstants.LOCALHOST;
import static io.debezium.server.redis.wip.TestConstants.MYSQL_PORT;
import static io.debezium.server.redis.wip.TestConstants.MYSQL_PRIVILEGED_PASSWORD;
import static io.debezium.server.redis.wip.TestConstants.MYSQL_PRIVILEGED_USER;
import static io.debezium.server.redis.wip.TestConstants.MYSQL_ROOT_PASSWORD;
import static io.debezium.server.redis.wip.TestConstants.REDIS_IMAGE;
import static io.debezium.server.redis.wip.TestConstants.REDIS_PORT;
import static io.debezium.server.redis.wip.TestProperties.DEBEZIUM_SERVER_IMAGE;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

public class TestContainersRedisTestBase {
    protected DebeziumTestContainerWrapper redis;
    protected DebeziumTestContainerWrapper server;
    protected DebeziumTestContainerWrapper mysql;
    protected Jedis jedis;

    public TestContainersRedisTestBase() {
        // provide base configuration for all components
        Network network = Network.newNetwork();

        mysql = new DebeziumTestContainerWrapper("quay.io/debezium/example-mysql")
                .withNetwork(network)
                .withNetworkAlias("mysql")
                .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections.*", 2))
                .withEnv("MYSQL_ROOT_PASSWORD", MYSQL_ROOT_PASSWORD)
                .withEnv("MYSQL_USER", MYSQL_PRIVILEGED_USER)
                .withEnv("MYSQL_PASSWORD", MYSQL_PRIVILEGED_PASSWORD)
                .withExposedPorts(MYSQL_PORT)
                .withStartupTimeout(Duration.ofSeconds(180));
        redis = new DebeziumTestContainerWrapper(REDIS_IMAGE)
                .withClasspathResourceMapping("redis", "/etc/redis", BindMode.READ_ONLY)
                .withNetwork(network)
                .withNetworkAlias("redis")
                .withExposedPorts(REDIS_PORT);
        server = new DebeziumTestContainerWrapper(DEBEZIUM_SERVER_IMAGE)
                .withNetwork(network)
                .withCommand("-jar", "quarkus-run.jar");
    }

    @BeforeEach
    public void setUp() {
        mysql.start();
        redis.start();
        jedis = new Jedis(HostAndPort.from(LOCALHOST + ":" + redis.getMappedPort(REDIS_PORT)));
    }

    @AfterEach
    public void tearDown() {
        server.stop();
        mysql.stop();
        redis.stop();
    }

    protected void startServerWithEnv(List<String> env) {
        server.setEnv(env);
        server.start();
    }

}
