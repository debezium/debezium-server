/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ydb;

import java.time.Duration;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import io.debezium.server.Images;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import tech.ydb.auth.NopAuthProvider;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;

/**
 * Single-node YDB for ITs. Fixed host port {@code localhost:2136} matches discovery hostnames
 * returned by the container.
 */
public class YdbTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(YdbTestResourceLifecycleManager.class);

    public static final String HOST = "localhost";
    public static final int GRPC_PORT = 2136;
    public static final String ENDPOINT = "grpc://" + HOST + ":" + GRPC_PORT;
    public static final String DATABASE = "/local";
    public static final String JDBC_USER = "debezium";
    public static final String JDBC_PASSWORD = "dbzpass";

    private static final Duration READY_TIMEOUT = Duration.ofMinutes(3);
    private static final long POLL_INTERVAL_MS = 5_000;

    private static final FixedHostPortGenericContainer<?> container = new FixedHostPortGenericContainer<>(Images.YDB_IMAGE)
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName(HOST).withPlatform("linux/amd64"))
            .withEnv("YDB_USE_IN_MEMORY_PDISKS", "true")
            .withEnv("YDB_DEFAULT_LOG_LEVEL", "NOTICE")
            .withEnv("GRPC_TLS_PORT", "2135")
            .withEnv("GRPC_PORT", String.valueOf(GRPC_PORT))
            .withEnv("MON_PORT", "8765")
            .withFixedExposedPort(2135, 2135)
            .withFixedExposedPort(8765, 8765)
            .withFixedExposedPort(GRPC_PORT, GRPC_PORT)
            .withStartupTimeout(READY_TIMEOUT);

    @Override
    public Map<String, String> start() {
        if (!container.isRunning()) {
            container.start();
            container.followOutput(new Slf4jLogConsumer(LOGGER));
        }
        try {
            waitForYdbReady();
            ensureDebeziumUser();
            ensurePrecreatedTopics();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        return Map.of();
    }

    private static void ensurePrecreatedTopics() {
        try (TestUtils.YdbAdmin admin = TestUtils.openAdmin()) {
            admin.createTopic(YdbTestConfigSource.prefixedTopic(YdbTestConfigSource.DESTINATION_CUSTOMERS),
                    YdbTestConfigSource.CONSUMER);
            admin.createTopic(YdbTestConfigSource.prefixedTopic(YdbTestConfigSource.DESTINATION_PRODUCTS),
                    YdbTestConfigSource.CONSUMER);
        }
    }

    @Override
    public void stop() {
        try {
            container.stop();
        }
        catch (Exception ignored) {
        }
    }

    public static void pause() {
        container.getDockerClient().pauseContainerCmd(container.getContainerId()).exec();
    }

    public static void unpause() {
        container.getDockerClient().unpauseContainerCmd(container.getContainerId()).exec();
    }

    public static void waitUntilReady() throws InterruptedException {
        waitForYdbReady();
    }

    private static void waitForYdbReady() throws InterruptedException {
        long deadline = System.currentTimeMillis() + READY_TIMEOUT.toMillis();
        Throwable lastError = null;
        String probeTable = "dbz_ydb_readiness_probe";
        String createDdl = "CREATE TABLE IF NOT EXISTS `" + probeTable + "` (id Utf8, PRIMARY KEY(id))";
        String dropDdl = "DROP TABLE IF EXISTS `" + probeTable + "`";
        Thread.sleep(POLL_INTERVAL_MS);
        while (System.currentTimeMillis() < deadline) {
            try (GrpcTransport transport = openTransport();
                    QueryClient queryClient = QueryClient.newClient(transport).build()) {
                SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();
                retryCtx.supplyResult(s -> s.createQuery(createDdl, TxMode.NONE).execute())
                        .join().getStatus().expectSuccess("readiness CREATE");
                retryCtx.supplyResult(s -> s.createQuery(dropDdl, TxMode.NONE).execute())
                        .join().getStatus().expectSuccess("readiness DROP");
                LOGGER.info("YDB ready");
                return;
            }
            catch (Throwable t) {
                lastError = t;
                Thread.sleep(POLL_INTERVAL_MS);
            }
        }
        throw new IllegalStateException("YDB did not become ready on " + ENDPOINT
                + " within " + READY_TIMEOUT + " (last error: " + lastError + ")", lastError);
    }

    private static void ensureDebeziumUser() {
        String createUser = "CREATE USER " + JDBC_USER + " PASSWORD '" + JDBC_PASSWORD + "'";
        String grantAll = "GRANT ALL ON `" + DATABASE + "` TO " + JDBC_USER;
        try (GrpcTransport transport = openTransport();
                QueryClient queryClient = QueryClient.newClient(transport).build()) {
            SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();
            runIgnoringAlreadyExists(retryCtx, createUser);
            retryCtx.supplyResult(s -> s.createQuery(grantAll, TxMode.NONE).execute())
                    .join().getStatus().expectSuccess("grant ALL on " + DATABASE + " to " + JDBC_USER);
            LOGGER.info("Ensured YDB user {} with ALL on {}", JDBC_USER, DATABASE);
        }
    }

    private static void runIgnoringAlreadyExists(SessionRetryContext retryCtx, String yql) {
        tech.ydb.core.Status status = retryCtx
                .supplyResult(s -> s.createQuery(yql, TxMode.NONE).execute())
                .join().getStatus();
        if (status.isSuccess()) {
            return;
        }
        if (status.getCode() == tech.ydb.core.StatusCode.PRECONDITION_FAILED
                || status.toString().toLowerCase().contains("already exists")) {
            return;
        }
        status.expectSuccess(yql);
    }

    private static GrpcTransport openTransport() {
        return GrpcTransport.forEndpoint(ENDPOINT, DATABASE)
                .withAuthProvider(NopAuthProvider.INSTANCE)
                .build();
    }
}
