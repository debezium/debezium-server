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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.doc.FixFor;
import io.smallrye.config.ConfigSourceContext;
import io.smallrye.config.ConfigValue;
import io.smallrye.config.EnvConfigSource;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;

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

    @Test
    @FixFor("debezium/dbz#2818")
    public void shouldExpandRemappedValuesOnlyOnce() {
        SmallRyeConfig config = config(Map.of(
                "debezium.transforms", "outbox",
                "debezium.transforms.outbox.route.topic.replacement", "outbox.event.\\${routedByValue}",
                "debezium.source.database.password", "$${file:secrets.properties:password}",
                "debezium.source.topic.prefix", "${prefix}",
                "prefix", "inventory"));

        assertThat(config.getValue("quarkus.debezium.transforms.outbox.route.topic.replacement", String.class)).isEqualTo("outbox.event.${routedByValue}");
        assertThat(config.getValue("quarkus.debezium.database.password", String.class)).isEqualTo("${file:secrets.properties:password}");
        assertThat(config.getValue("quarkus.debezium.topic.prefix", String.class)).isEqualTo("inventory");
    }

    @ParameterizedTest
    @FixFor("debezium/dbz#2818")
    @ValueSource(strings = {
            "outbox.event.\\${routedByValue}",
            "outbox.event.$${routedByValue}",
            "outbox.event.$\\$${routedByValue}",
            "$${file:secrets.properties:password}",
            "${prefix}",
            "${prefix}.${prefix}",
            "${missing:fallback}",
            "${missing:${prefix}}",
            "${prefix:}.events",
            "price$5",
            "price$",
            "a$$b",
            "a$$$b",
            "a$$$$b",
            "$}",
            "$:",
            "\\$",
            "^\\d+$",
            "C:\\temp",
            "${prefix}\\"
    })
    public void shouldReadRemappedValueAsOriginalValue(String value) {
        SmallRyeConfig config = config(Map.of(
                "debezium.source.remapped", value,
                "quarkus.debezium.bridged", value,
                "debezium.transforms", "outbox",
                "debezium.transforms.outbox.remapped", value,
                "prefix", "inventory"),
                Map.of("DEBEZIUM_SOURCE_FROMENV", value));

        assertThat(config.getValue("quarkus.debezium.remapped", String.class)).isEqualTo(config.getValue("debezium.source.remapped", String.class));
        assertThat(config.getValue("debezium.source.bridged", String.class)).isEqualTo(config.getValue("quarkus.debezium.bridged", String.class));
        assertThat(config.getValue("quarkus.debezium.transforms.outbox.remapped", String.class))
                .isEqualTo(config.getValue("debezium.transforms.outbox.remapped", String.class));
        assertThat(config.getValue("quarkus.debezium.fromenv", String.class)).isEqualTo(config.getValue("debezium.source.fromenv", String.class));
    }

    @Test
    @FixFor("debezium/dbz#2818")
    public void shouldReuseSinkPropertiesWhenSinkTypeIsExpression() {
        SmallRyeConfig config = config(Map.of(
                "debezium.sink.type", "${sink}",
                "sink", "redis",
                "debezium.sink.redis.address", "localhost:6379"));

        assertThat(config.getValue("quarkus.debezium.name", String.class)).isEqualTo("redis");
        assertThat(config.getValue("quarkus.debezium.schema.history.internal.redis.address", String.class)).isEqualTo("localhost:6379");
        assertThat(config.getValue("quarkus.debezium.offset.storage.redis.address", String.class)).isEqualTo("localhost:6379");
    }

    @Test
    @FixFor("debezium/dbz#2818")
    public void shouldPreserveEmptyValueFromExpression() {
        SmallRyeConfig config = config(Map.of("debezium.source.table.include.list", "${tables:}"));

        assertThat(config.getValue("quarkus.debezium.table.include.list", String.class)).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2818")
    public void shouldResolveReferenceToBridgedProperty() {
        SmallRyeConfig config = config(Map.of(
                "debezium.source.offset.flush.interval.ms", "${quarkus.debezium.offset.flush.interval.ms:1000}",
                "quarkus.debezium.snapshot.fetch.size", "${debezium.source.snapshot.fetch.size:500}"));

        assertThat(config.getValue("debezium.source.offset.flush.interval.ms", String.class)).isEqualTo("1000");
        assertThat(config.getValue("quarkus.debezium.offset.flush.interval.ms", String.class)).isEqualTo("1000");
        assertThat(config.getValue("quarkus.debezium.snapshot.fetch.size", String.class)).isEqualTo("500");
        assertThat(config.getValue("debezium.source.snapshot.fetch.size", String.class)).isEqualTo("500");
    }

    private static SmallRyeConfig config(Map<String, String> properties) {
        return config(properties, Map.of());
    }

    private static SmallRyeConfig config(Map<String, String> properties, Map<String, String> env) {
        return new SmallRyeConfigBuilder()
                .addDefaultInterceptors()
                .withConverters(new EmptyStringConverter[]{ new EmptyStringConverter() })
                .withSources(new PropertiesConfigSource(properties, "test", 300))
                .withSources(new EnvConfigSource(env, 300))
                .withSources(new DebeziumServerConfigSourceFactory())
                .build();
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
