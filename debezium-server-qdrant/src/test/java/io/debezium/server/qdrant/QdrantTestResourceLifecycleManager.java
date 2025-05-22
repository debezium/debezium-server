/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.qdrant;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.testcontainers.qdrant.QdrantContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.DebeziumException;
import io.debezium.server.Images;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Collections.Distance;
import io.qdrant.client.grpc.Collections.VectorParams;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class QdrantTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    public static final int QDRANT_RPC_PORT = 6334;

    @SuppressWarnings("resource")
    private static final QdrantContainer container = new QdrantContainer(DockerImageName.parse(Images.QDRANT_IMAGE).asCompatibleSubstituteFor("qdrant/qdrant"))
            .withStartupTimeout(Duration.ofSeconds(90));

    @Override
    public Map<String, String> start() {
        container.start();
        createQdrantCollections();

        Map<String, String> params = new ConcurrentHashMap<>();
        params.put("debezium.sink.qdrant.host", "localhost");
        params.put("debezium.sink.qdrant.port", getPort());

        return params;
    }

    @Override
    public void stop() {
        try {
            if (container != null) {
                container.stop();
                container.close();
            }
        }
        catch (Exception e) {
            // ignored
        }
    }

    private void createQdrantCollections() {
        try (var client = new QdrantClient(QdrantGrpcClient.newBuilder(
                Grpc.newChannelBuilder("%s:%s".formatted("localhost", getPort()), InsecureChannelCredentials.create())
                        .build(),
                true)
                .build());) {
            client.createCollectionAsync(QdrantIT.COLLECTION_NAME,
                    VectorParams.newBuilder().setDistance(Distance.Euclid).setSize(3).build()).get();
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    protected String getPort() {
        return Integer.toString(container.getMappedPort(QDRANT_RPC_PORT));
    }

}
