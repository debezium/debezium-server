/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.milvus;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.testcontainers.milvus.MilvusContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.server.Images;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class MilvusTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    @SuppressWarnings("resource")
    private static final MilvusContainer container = new MilvusContainer(DockerImageName.parse(Images.MILVUS_IMAGE).asCompatibleSubstituteFor("milvusdb/milvus"))
            .withStartupTimeout(Duration.ofSeconds(90));

    @Override
    public Map<String, String> start() {
        container.start();

        Map<String, String> params = new ConcurrentHashMap<>();
        params.put("debezium.sink.milvus.uri", "http://localhost:19530");

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
}
