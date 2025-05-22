/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.qdrant;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.event.Observes;

import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.server.TestConfigSource;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.util.Testing;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.qdrant.client.ConditionFactory;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Points.Filter;
import io.qdrant.client.grpc.Points.QueryPoints;
import io.qdrant.client.grpc.Points.ScoredPoint;
import io.qdrant.client.grpc.Points.WithPayloadSelector;
import io.qdrant.client.grpc.Points.WithVectorsSelector;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to a Qdrant vector database.
 *
 * @author Jiri Pechanec
 */
@QuarkusTest
@QuarkusTestResource(VectorPostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(QdrantTestResourceLifecycleManager.class)
public class QdrantIT {

    private static final int MESSAGE_COUNT = 2;
    public static final String COLLECTION_NAME = "testc.inventory.t_vector";

    @ConfigProperty(name = "debezium.source.database.hostname")
    String dbHostname;

    @ConfigProperty(name = "debezium.source.database.port")
    String dbPort;

    @ConfigProperty(name = "debezium.source.database.user")
    String dbUser;

    @ConfigProperty(name = "debezium.source.database.password")
    String dbPassword;

    @ConfigProperty(name = "debezium.source.database.dbname")
    String dbName;

    @ConfigProperty(name = "debezium.sink.qdrant.host")
    String qdrantHost;

    @ConfigProperty(name = "debezium.sink.qdrant.port")
    String qdrantPort;

    private QdrantClient client;

    {
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(QdrantTestConfigSource.OFFSET_STORE_PATH);
    }

    @BeforeEach
    void setupDependencies() throws Exception {
        Testing.Print.enable();

        client = new QdrantClient(QdrantGrpcClient.newBuilder(
                Grpc.newChannelBuilder("%s:%s".formatted(qdrantHost, qdrantPort), InsecureChannelCredentials.create())
                        .build(),
                true)
                .build());
    }

    @AfterEach
    void cleanDependencies() throws Exception {
        Testing.Print.enable();

        if (client != null) {
            client.close();
        }
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw new RuntimeException(event.getError().get());
        }
    }

    @Test
    public void testQdrant() throws Exception {
        final var queryResults = new AtomicReference<List<ScoredPoint>>();
        Awaitility.await().atMost(Duration.ofSeconds(QdrantTestConfigSource.waitForSeconds())).until(() -> {
            final var points = client.queryAsync(QueryPoints.newBuilder()
                    .setCollectionName(COLLECTION_NAME)
                    .setWithPayload(WithPayloadSelector.newBuilder()
                            .setEnable(true)
                            .build())
                    .setWithVectors(WithVectorsSelector.newBuilder()
                            .setEnable(true)
                            .build())
                    .build()).get();
            queryResults.set(points);
            return points.size() == MESSAGE_COUNT;
        });

        assertThat(queryResults.get()).hasSize(MESSAGE_COUNT);
        final var dataRead1 = queryResults.get().get(0);
        final var dataRead2 = queryResults.get().get(1);

        assertThat(dataRead1.getId().getNum()).isEqualTo(1l);
        assertThat(dataRead1.getPayloadMap().get("value").getStringValue()).isEqualTo("one");
        assertThat(dataRead1.getVectors().getVector().getDataList()).isEqualTo(List.of(1.1f, 1.2f, 1.3f));
        assertThat(dataRead1.getPayloadMap().get("f_json").getStringValue()).isEqualTo("{}");

        assertThat(dataRead2.getId().getNum()).isEqualTo(2l);
        assertThat(dataRead2.getPayloadMap().get("value").getStringValue()).isEqualTo("two");
        assertThat(dataRead2.getVectors().getVector().getDataList()).isEqualTo(List.of(2.1f, 2.2f, 2.3f));
        assertThat(dataRead2.getPayloadMap().get("f_json").getStringValue()).isEqualTo("{}");

        final JdbcConfiguration config = JdbcConfiguration.create()
                .with("hostname", dbHostname)
                .with("port", dbPort)
                .with("user", dbUser)
                .with("password", dbPassword)
                .with("dbname", dbName)
                .build();
        try (PostgresConnection connection = new PostgresConnection(config, "Debezium Qdrant Test")) {
            connection.execute("UPDATE inventory.t_vector SET value = 'two-up' WHERE pk = 2");
        }

        Awaitility.await().atMost(Duration.ofSeconds(QdrantTestConfigSource.waitForSeconds())).until(() -> {
            final var points = client.queryAsync(QueryPoints.newBuilder()
                    .setCollectionName(COLLECTION_NAME)
                    .setFilter(Filter.newBuilder()
                            .addMust(ConditionFactory.matchText("value", "two-up"))
                            .build())
                    .setWithPayload(WithPayloadSelector.newBuilder()
                            .setEnable(true)
                            .build())
                    .setWithVectors(WithVectorsSelector.newBuilder()
                            .setEnable(true)
                            .build())
                    .build()).get();
            queryResults.set(points);
            return points.size() == 1;
        });

        assertThat(queryResults.get()).hasSize(1);
        final var dataUpdate2 = queryResults.get().get(0);

        assertThat(dataUpdate2.getId().getNum()).isEqualTo(2l);
        assertThat(dataUpdate2.getPayloadMap().get("value").getStringValue()).isEqualTo("two-up");
        assertThat(dataUpdate2.getVectors().getVector().getDataList()).isEqualTo(List.of(2.1f, 2.2f, 2.3f));
        assertThat(dataUpdate2.getPayloadMap().get("f_json").getStringValue()).isEqualTo("{}");

        try (PostgresConnection connection = new PostgresConnection(config, "Debezium Qdrant Test")) {
            connection.execute("DELETE FROM inventory.t_vector WHERE pk = 2");
        }
        Awaitility.await().atMost(Duration.ofSeconds(QdrantTestConfigSource.waitForSeconds())).until(() -> {
            final var points = client.queryAsync(QueryPoints.newBuilder()
                    .setCollectionName(COLLECTION_NAME)
                    .setFilter(Filter.newBuilder()
                            .addMust(ConditionFactory.matchText("value", "two-up"))
                            .build())
                    .setWithPayload(WithPayloadSelector.newBuilder()
                            .setEnable(true)
                            .build())
                    .setWithVectors(WithVectorsSelector.newBuilder()
                            .setEnable(true)
                            .build())
                    .build()).get();
            queryResults.set(points);
            return points.size() == 0;
        });

        assertThat(queryResults.get()).hasSize(0);
    }
}
