/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.milvus;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.testcontainers.milvus.MilvusContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.server.Images;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq.CollectionSchema;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class MilvusTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    public static final int MILVUS_PORT = 19530;

    @SuppressWarnings("resource")
    private static final MilvusContainer container = new MilvusContainer(DockerImageName.parse(Images.MILVUS_IMAGE).asCompatibleSubstituteFor("milvusdb/milvus"))
            .withStartupTimeout(Duration.ofSeconds(90));

    @Override
    public Map<String, String> start() {
        container.start();
        createMilvusCollections();

        Map<String, String> params = new ConcurrentHashMap<>();
        params.put("debezium.sink.milvus.uri", getMilvusUri());

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

    private void createMilvusCollections() {
        final var config = ConnectConfig.builder()
                .uri(getMilvusUri())
                .build();
        final var client = new MilvusClientV2(config);

        try {
            client.dropCollection(DropCollectionReq.builder().collectionName(MilvusIT.COLLECTION_NAME).build());
        }
        catch (Exception e) {
            // Ignore drop errors for non-existing collection
        }
        final var collections = client.listCollections().getCollectionNames();
        if (!collections.contains(MilvusIT.COLLECTION_NAME)) {
            final var pkField = CreateCollectionReq.FieldSchema.builder()
                    .name("pk")
                    .isPrimaryKey(true)
                    .dataType(DataType.Int64)
                    .build();
            final var valueField = CreateCollectionReq.FieldSchema.builder()
                    .name("value")
                    .dataType(DataType.VarChar)
                    .build();
            final var vectorField = CreateCollectionReq.FieldSchema.builder()
                    .name("f_vector")
                    .dataType(DataType.FloatVector)
                    .dimension(3)
                    .build();
            final var collectionSchema = CollectionSchema.builder()
                    .fieldSchemaList(List.of(pkField, valueField, vectorField))
                    .build();
            final var index = IndexParam.builder()
                    .fieldName("f_vector")
                    .indexType(IndexParam.IndexType.AUTOINDEX)
                    .build();
            final var request = CreateCollectionReq.builder()
                    .collectionName(MilvusIT.COLLECTION_NAME)
                    .collectionSchema(collectionSchema)
                    .indexParams(List.of(index))
                    .build();
            client.createCollection(request);
        }

        client.close();
    }

    public static String getMilvusUri() {
        return "http://localhost:" + container.getMappedPort(MILVUS_PORT);
    }
}
