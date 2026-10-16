/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.elasticsearch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusTestProfile;

/**
 * Quarkus test profile for the Elasticsearch sink integration tests with a PostgreSQL source.
 * The Elasticsearch connection properties are contributed by
 * {@link ElasticsearchTestResourceLifecycleManager} once the container is running.
 *
 * @author Chris Cranford
 */
public class ElasticsearchTestProfile implements QuarkusTestProfile {

    @Override
    public List<TestResourceEntry> testResources() {
        return List.of(
                new TestResourceEntry(PostgresTestResourceLifecycleManager.class),
                new TestResourceEntry(ElasticsearchTestResourceLifecycleManager.class));
    }

    @Override
    public Map<String, String> getConfigOverrides() {
        final Map<String, String> config = new HashMap<>();

        // Sink type
        config.put("debezium.sink.type", "elasticsearch");

        // Elasticsearch sink behavior
        config.put("debezium.sink.elasticsearch.primary.key.mode", "record_key");
        config.put("debezium.sink.elasticsearch.write.method", "upsert");
        config.put("debezium.sink.elasticsearch.delete.enabled", "true");
        config.put("debezium.sink.elasticsearch.batch.size", "100");
        config.put("debezium.sink.elasticsearch.linger.ms", "0");

        // Source configuration
        config.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                TestConfigSource.OFFSET_STORE_PATH.toAbsolutePath().toString());
        config.put("debezium.source.offset.flush.interval.ms", "0");
        config.put("debezium.source.topic.prefix", "testserver");
        config.put("debezium.source.schema.include.list", "inventory");
        config.put("debezium.source.table.include.list", "inventory.customers,inventory.products");

        return config;
    }
}
