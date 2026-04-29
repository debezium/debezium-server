/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.fluss;

import static io.debezium.server.Images.FLUSS_IMAGE;
import static io.debezium.server.Images.FLUSS_ZOOKEEPER_IMAGE;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.StringType;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/**
 * Manages the lifecycle of a Fluss cluster (ZooKeeper + coordinator + tablet server) for integration testing.
 *
 * <p>Uses host networking so that the Fluss coordinator and tablet server register with addresses
 * reachable by the test JVM without port-mapping complications.
 */
public class FlussTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    private static final int COORDINATOR_PORT = 9123;
    private static final int TABLET_PORT = 10123;
    private static final int ZK_PORT = 2181;

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:%d".formatted(COORDINATOR_PORT);
    private static final String COORDINATOR_FLUSS_PROPERTIES = flussProperties(COORDINATOR_PORT);
    private static final String TABLET_FLUSS_PROPERTIES = flussProperties(TABLET_PORT);

    private static String flussProperties(int bindPort) {
        return "bind.listeners: FLUSS://127.0.0.1:%d\nzookeeper.address: 127.0.0.1:%d".formatted(bindPort, ZK_PORT);
    }

    @SuppressWarnings("resource")
    private static final GenericContainer<?> zookeeper = new GenericContainer<>(DockerImageName.parse(FLUSS_ZOOKEEPER_IMAGE))
            .withNetworkMode("host")
            .waitingFor(Wait.forLogMessage(".*binding to port.*", 1));

    @SuppressWarnings("resource")
    private static final GenericContainer<?> coordinator = new GenericContainer<>(DockerImageName.parse(FLUSS_IMAGE))
            .withNetworkMode("host")
            .withEnv("FLUSS_PROPERTIES", COORDINATOR_FLUSS_PROPERTIES)
            .withCommand("coordinatorServer")
            .waitingFor(Wait.forLogMessage(".*End initializing coordinator context.*", 1));

    @SuppressWarnings("resource")
    private static final GenericContainer<?> tablet = new GenericContainer<>(DockerImageName.parse(FLUSS_IMAGE))
            .withNetworkMode("host")
            .withEnv("FLUSS_PROPERTIES", TABLET_FLUSS_PROPERTIES)
            .withCommand("tabletServer")
            .waitingFor(Wait.forLogMessage(".*Registered tablet server.*", 1));

    private static final AtomicBoolean running = new AtomicBoolean(false);

    private static synchronized void init() {
        if (!running.get()) {
            zookeeper.start();
            coordinator.start();
            tablet.start();
            createTestTables();
            running.set(true);
        }
    }

    private static void createTestTables() {
        final Configuration flussConfig = new Configuration();
        flussConfig.setString("bootstrap.servers", BOOTSTRAP_SERVERS);

        try (Connection connection = ConnectionFactory.createConnection(flussConfig); Admin admin = connection.getAdmin()) {
            // Log table (no primary key), used by FlussIT and FlussConnectFormatIT.
            // Pre-created so those tests do not depend on auto-create or schema-enabled JSON format.
            createTableIfAbsent(admin, TablePath.of(FlussTestConfigSource.DEFAULT_DATABASE, FlussIT.TABLE_NAME),
                    Schema.newBuilder()
                            .column("id", new IntType())
                            .column("first_name", new StringType())
                            .column("last_name", new StringType())
                            .column("email", new StringType())
                            .build());

            // Primary-key table, used by FlussUpsertModeIT to exercise upsert mode end-to-end.
            createTableIfAbsent(admin, TablePath.of(FlussTestConfigSource.DEFAULT_DATABASE, FlussUpsertModeIT.TABLE_NAME),
                    Schema.newBuilder()
                            .column("id", new IntType())
                            .column("first_name", new StringType())
                            .column("last_name", new StringType())
                            .column("email", new StringType())
                            .primaryKey("id")
                            .build());
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to pre-create Fluss test tables", e);
        }
    }

    private static void createTableIfAbsent(Admin admin, TablePath tablePath, Schema schema) throws Exception {
        if (!admin.tableExists(tablePath).get()) {
            admin.createTable(tablePath, TableDescriptor.builder().schema(schema).build(), false).get();
        }
    }

    @Override
    public Map<String, String> start() {
        init();

        final Map<String, String> params = new ConcurrentHashMap<>();
        params.put("debezium.sink.fluss.bootstrap.servers", BOOTSTRAP_SERVERS);
        return params;
    }

    @Override
    public void stop() {
        stopQuietly(tablet);
        stopQuietly(coordinator);
        stopQuietly(zookeeper);
    }

    private static void stopQuietly(GenericContainer<?> container) {
        try {
            container.stop();
        }
        catch (Exception e) {
            // ignored
        }
    }

    public static String getBootstrapServers() {
        return BOOTSTRAP_SERVERS;
    }
}