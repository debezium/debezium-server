package io.debezium.server.redis;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.debezium.server.TestConfigSource;
import io.debezium.server.redis.lifecyclemanagers.RedisTestResourceLifecycleManager;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static io.debezium.server.redis.lifecyclemanagers.RedisTestResourceLifecycleManager.REDIS_PORT;
import static io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager.POSTGRES_DBNAME;
import static io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager.POSTGRES_PASSWORD;
import static io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager.POSTGRES_PORT;
import static io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager.POSTGRES_USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.output.OutputFrame.OutputType.STDOUT;


public class TestContainersPrototype {

    private final GenericContainer<?> server = new GenericContainer<>("quay.io/debezium/server:nightly")
            .waitingFor(Wait.forLogMessage(".*debezium.*", 1));
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
                "debezium.source.offset.storage.file.filename=" + TestConfigSource.OFFSET_STORE_PATH));
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

        server.stop();
    }

    @Test
    public void runServerFromJar() throws IOException, URISyntaxException, InterruptedException {
        // Run a java app in a separate system process
        String command = "/home/martinmedek/.sdkman/candidates/java/17.0.2-open/bin/java" +
                " -Dquarkus.http.port=8081 " +
                "-Dquarkus.http.ssl-port=8444 " +
                "-Dtest.url=http://localhost:8081 " +
                "-Dquarkus.log.file.path=" + getTargetPath() + "/quarkus.log " +
                "-Dquarkus.log.file.enable=true " +
                "-Dquarkus.log.category.\"io.quarkus\".level=INFO " +
                "-Ddebezium.source.schema.include.list=inventory " +
                "-Ddebezium.source.database.port=" + PostgresTestResourceLifecycleManager.getContainer().getMappedPort(5432) + " " +
                "-Ddebezium.sink.type=redis " +
                "-Ddebezium.source.table.include.list=inventory.customers,inventory.redis_test,inventory.redis_test2 " +
                "-Ddebezium.sink.redis.address=localhost:" + RedisTestResourceLifecycleManager.getContainer().getMappedPort(6379) + " " +
                "-Ddebezium.source.database.user=postgres " +
                "-Ddebezium.source.database.dbname=postgres " +
                "-Ddebezium.source.offset.storage.file.filename=" + getTargetPath() + "/data/file-connector-offsets.txt " +
                "-Ddebezium.source.database.hostname=localhost " +
                "-Ddebezium.source.database.password=postgres " +
                "-Ddebezium.source.topic.prefix=testc -Ddebezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector " +
                "-Dquarkus.configuration.build-time-mismatch-at-runtime=fail " +
                "-Ddebezium.source.offset.flush.interval.ms=0 " +
                "-jar " + getTargetPath() + "/quarkus-app/quarkus-run.jar";

        Process proc = Runtime.getRuntime().exec(command);
        InputStream in = proc.getInputStream();
        InputStream err = proc.getErrorStream();
//        in.transferTo(System.out);
//        err.transferTo(System.out);

        Jedis jedis = new Jedis(HostAndPort.from(RedisTestResourceLifecycleManager.getRedisContainerAddress()));
        final int MESSAGE_COUNT = 4;
        final String STREAM_NAME = "testc.inventory.customers";
        TestUtils.awaitStreamLengthGte(jedis, STREAM_NAME, MESSAGE_COUNT);

        PostgresTestResourceLifecycleManager.getContainer().execInContainer("psql",
                "-U", PostgresTestResourceLifecycleManager.POSTGRES_USER,
                "-d", PostgresTestResourceLifecycleManager.POSTGRES_DBNAME,
                "-c", "INSERT INTO inventory.customers VALUES (1005, 'aaa','aaa','aa@example.com')");

        TestUtils.awaitStreamLengthGte(jedis, STREAM_NAME, MESSAGE_COUNT + 1);
        proc.destroy();

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
                    } else {
                        System.out.println(l);
                    }
                }
        );
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
}
