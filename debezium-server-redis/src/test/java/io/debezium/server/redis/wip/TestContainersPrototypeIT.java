/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis.wip;

import static io.debezium.server.redis.RedisTestResourceLifecycleManager.REDIS_PORT;
import static io.debezium.server.redis.wip.TestProperties.DEBEZIUM_SERVER_IMAGE_GROUP;
import static io.debezium.server.redis.wip.TestProperties.DEBEZIUM_VERSION;
import static io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager.POSTGRES_DBNAME;
import static io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager.POSTGRES_PASSWORD;
import static io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager.POSTGRES_PORT;
import static io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager.POSTGRES_USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.output.OutputFrame.OutputType.STDOUT;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import io.debezium.server.redis.RedisTestResourceLifecycleManager;
import io.debezium.server.redis.TestUtils;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

public class TestContainersPrototypeIT {

    private final PostgresTestResourceLifecycleManager postgres = new PostgresTestResourceLifecycleManager();
    private final RedisTestResourceLifecycleManager redis = new RedisTestResourceLifecycleManager();

    @BeforeEach
    public void setUp() {
        postgres.start();
        redis.start();
    }

    @AfterEach
    public void tearDown() {
        postgres.stop();
        redis.stop();
    }

    @Test
    public void runServerContainer() throws InterruptedException, IOException {
        try (
                GenericContainer<?> server = new GenericContainer<>(DEBEZIUM_SERVER_IMAGE_GROUP + "/debezium-server-redis:" + DEBEZIUM_VERSION)
                        .waitingFor(Wait.forLogMessage(".*debezium.*", 1))) {

            server.setEnv(List.of("debezium.sink.type=redis",
                    "debezium.sink.redis.address=" + getContainerIp(RedisTestResourceLifecycleManager.getContainer()) + ":" + REDIS_PORT,
                    "debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector",
                    "debezium.source.offset.flush.interval.ms=0",
                    "debezium.source.topic.prefix=testc",
                    "debezium.source.schema.include.list=inventory",
                    "debezium.source.database.hostname=" + getContainerIp(PostgresTestResourceLifecycleManager.getContainer()),
                    "debezium.source.database.port=" + POSTGRES_PORT,
                    "debezium.source.database.user=" + POSTGRES_USER,
                    "debezium.source.database.password=" + POSTGRES_PASSWORD,
                    "debezium.source.database.dbname=" + POSTGRES_DBNAME,
                    "debezium.source.offset.storage.file.filename=" + "/offset.dat"));
            server.start();
            Jedis jedis = new Jedis(HostAndPort.from(RedisTestResourceLifecycleManager.getRedisContainerAddress()));
            final int MESSAGE_COUNT = 4;
            final String STREAM_NAME = "testc.inventory.customers";

            TestUtils.awaitStreamLengthGte(jedis, STREAM_NAME, MESSAGE_COUNT);
            assertThat(server.getLogs(STDOUT)).contains("inventory.customers");

            PostgresTestResourceLifecycleManager.getContainer().execInContainer("psql",
                    "-U", PostgresTestResourceLifecycleManager.POSTGRES_USER,
                    "-d", PostgresTestResourceLifecycleManager.POSTGRES_DBNAME,
                    "-c", "INSERT INTO inventory.customers VALUES (1005, 'aaa','aaaa','aa@example.com')");

            TestUtils.awaitStreamLengthGte(jedis, STREAM_NAME, MESSAGE_COUNT + 1);
            System.out.println(server.getLogs(STDOUT));
            server.stop();
        }
    }

    private void printLogs(GenericContainer<?> container) {
        String logs = container.getLogs();
        Arrays.asList(logs.split("\n")).forEach(
                l -> {
                    if (l.startsWith("{")) {
                        Gson gson = new GsonBuilder().setPrettyPrinting().create();
                        JsonElement el = JsonParser.parseString(l).getAsJsonObject().get("message");
                        String jsonString = gson.toJson(el);
                        System.out.println(jsonString);
                    }
                    else {
                        System.out.println(l);
                    }
                });
    }

    private String getContainerIp(GenericContainer<?> container) {
        return container
                .getContainerInfo()
                .getNetworkSettings()
                .getNetworks()
                .entrySet()
                .stream()
                .findFirst()
                .get()
                .getValue()
                .getIpAddress();
    }

    private String getTargetPath() throws URISyntaxException {
        return Paths.get(getClass().getResource("/").toURI()).getParent().toString();
    }

    ;
}
