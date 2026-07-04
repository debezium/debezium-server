/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.smallrye.config.ConfigSourceContext;
import io.smallrye.config.ConfigValue;

/**
 * Unit tests for the reuse of the sink connection properties for the schema history and offset
 * storage namespaces in {@link DebeziumServerConfigSourceFactory}.
 *
 * @author Mahitha Adapa
 */
public class DebeziumServerConfigSourceFactoryTest {

    @Test
    public void shouldReuseSinkPropertiesForStorageWhenStorageNotConfigured() {
        Map<String, String> result = remap(Map.of(
                "debezium.sink.type", "redis",
                "debezium.sink.redis.address", "localhost:6379"));

        assertThat(result).containsEntry("quarkus.debezium.schema.history.internal.redis.address", "localhost:6379");
        assertThat(result).containsEntry("quarkus.debezium.offset.storage.redis.address", "localhost:6379");
    }

    @Test
    public void shouldNotReuseSinkPropertiesWhenSchemaHistoryConfiguredExplicitly() {
        Map<String, String> result = remap(Map.of(
                "debezium.sink.type", "redis",
                "debezium.sink.redis.connection.url", "sink-host:6379",
                "debezium.sink.redis.batch.size", "1000",
                "debezium.source.schema.history.internal.redis.url", "history-host:6379"));

        // Explicit config is kept and sink properties with different names do not leak into the namespace.
        assertThat(result).containsEntry("quarkus.debezium.schema.history.internal.redis.url", "history-host:6379");
        assertThat(result).doesNotContainKey("quarkus.debezium.schema.history.internal.redis.connection.url");
        assertThat(result).doesNotContainKey("quarkus.debezium.schema.history.internal.redis.batch.size");
        // The guard is namespace-specific: offset storage is still reused.
        assertThat(result).containsEntry("quarkus.debezium.offset.storage.redis.connection.url", "sink-host:6379");
    }

    @Test
    public void shouldNotReuseSinkPropertiesWhenSchemaHistoryConfiguredExplicitlyWithQuarkusNamespace() {
        Map<String, String> result = remap(Map.of(
                "debezium.sink.type", "redis",
                "debezium.sink.redis.connection.url", "sink-host:6379",
                "debezium.sink.redis.batch.size", "1000",
                "quarkus.debezium.schema.history.internal.redis.url", "history-host:6379"));

        assertThat(result).containsEntry("debezium.source.schema.history.internal.redis.url", "history-host:6379");
        assertThat(result).doesNotContainKey("quarkus.debezium.schema.history.internal.redis.connection.url");
        assertThat(result).doesNotContainKey("quarkus.debezium.schema.history.internal.redis.batch.size");
        // The guard is namespace-specific: offset storage is still reused.
        assertThat(result).containsEntry("quarkus.debezium.offset.storage.redis.connection.url", "sink-host:6379");
    }

    @Test
    public void shouldNotReuseSinkPropertiesWhenOffsetStorageConfiguredExplicitly() {
        Map<String, String> result = remap(Map.of(
                "debezium.sink.type", "redis",
                "debezium.sink.redis.address", "sink-host:6379",
                "debezium.sink.redis.batch.size", "1000",
                "debezium.source.offset.storage.redis.address", "offset-host:6379"));

        assertThat(result).containsEntry("quarkus.debezium.offset.storage.redis.address", "offset-host:6379");
        assertThat(result).doesNotContainKey("quarkus.debezium.offset.storage.redis.batch.size");
        // The guard is namespace-specific: schema history is still reused.
        assertThat(result).containsEntry("quarkus.debezium.schema.history.internal.redis.address", "sink-host:6379");
    }

    private static Map<String, String> remap(Map<String, String> input) {
        Map<String, String> result = new HashMap<>();
        new DebeziumServerConfigSourceFactory().getConfigSources(new MapConfigSourceContext(input))
                .forEach(source -> result.putAll(source.getProperties()));
        return result;
    }

    /**
     * Minimal {@link ConfigSourceContext} backed by a fixed map, so the factory can be exercised
     * without the full MicroProfile Config runtime.
     */
    private static final class MapConfigSourceContext implements ConfigSourceContext {

        private final Map<String, String> properties;

        MapConfigSourceContext(Map<String, String> properties) {
            this.properties = properties;
        }

        @Override
        public ConfigValue getValue(String name) {
            return ConfigValue.builder().withName(name).withValue(properties.get(name)).build();
        }

        @Override
        public Iterator<String> iterateNames() {
            return properties.keySet().iterator();
        }
    }
}
