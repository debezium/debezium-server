/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.quarkus.bridge.deployment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.eclipse.microprofile.config.spi.ConfigSource;
import org.junit.jupiter.api.Test;

import io.debezium.quarkus.bridge.DebeziumBridgeConfigSourceFactory;
import io.smallrye.config.ConfigSourceContext;
import io.smallrye.config.ConfigValue;

class DebeziumBridgeConfigSourceFactoryTest {

    private final DebeziumBridgeConfigSourceFactory factory = new DebeziumBridgeConfigSourceFactory();

    @Test
    void remapsQuarkusDebeziumSinkProperties() {
        ConfigSourceContext context = createContext(Map.of(
                "quarkus.debezium.sink.type", "http",
                "quarkus.debezium.sink.http.url", "http://localhost:8080"));

        ConfigSource source = first(factory.getConfigSources(context));

        assertThat(source.getValue("debezium.sink.type")).isEqualTo("http");
        assertThat(source.getValue("debezium.sink.http.url")).isEqualTo("http://localhost:8080");
    }

    @Test
    void preservesSuffixStructure() {
        ConfigSourceContext context = createContext(Map.of(
                "quarkus.debezium.sink.redis.address", "redis://localhost:6379"));

        ConfigSource source = first(factory.getConfigSources(context));

        assertThat(source.getValue("debezium.sink.redis.address")).isEqualTo("redis://localhost:6379");
    }

    @Test
    void doesNotRemapNonSinkProperties() {
        ConfigSourceContext context = createContext(Map.of(
                "quarkus.datasource.url", "jdbc:postgresql://localhost/db",
                "some.other.property", "value"));

        ConfigSource source = first(factory.getConfigSources(context));

        assertThat(source.getProperties()).isEmpty();
    }

    @Test
    void skipsPropertiesWithNullConfigValue() {
        ConfigSourceContext context = mock(ConfigSourceContext.class);
        when(context.iterateNames()).thenReturn(
                java.util.List.of("quarkus.debezium.sink.type", "quarkus.debezium.sink.missing").iterator());

        ConfigValue typeValue = mock(ConfigValue.class);
        when(typeValue.getValue()).thenReturn("http");
        when(context.getValue("quarkus.debezium.sink.type")).thenReturn(typeValue);
        when(context.getValue("quarkus.debezium.sink.missing")).thenReturn(null);

        ConfigSource source = first(factory.getConfigSources(context));

        assertThat(source.getValue("debezium.sink.type")).isEqualTo("http");
        assertThat(source.getValue("debezium.sink.missing")).isNull();
    }

    @Test
    void skipsPropertiesWithNullInnerValue() {
        ConfigSourceContext context = mock(ConfigSourceContext.class);
        when(context.iterateNames()).thenReturn(
                java.util.List.of("quarkus.debezium.sink.type", "quarkus.debezium.sink.empty").iterator());

        ConfigValue typeValue = mock(ConfigValue.class);
        when(typeValue.getValue()).thenReturn("http");
        when(context.getValue("quarkus.debezium.sink.type")).thenReturn(typeValue);

        ConfigValue nullInnerValue = mock(ConfigValue.class);
        when(nullInnerValue.getValue()).thenReturn(null);
        when(context.getValue("quarkus.debezium.sink.empty")).thenReturn(nullInnerValue);

        ConfigSource source = first(factory.getConfigSources(context));

        assertThat(source.getValue("debezium.sink.type")).isEqualTo("http");
        assertThat(source.getValue("debezium.sink.empty")).isNull();
    }

    @Test
    void configSourceHasCorrectName() {
        ConfigSourceContext context = createContext(Map.of("quarkus.debezium.sink.type", "http"));

        ConfigSource source = first(factory.getConfigSources(context));

        assertThat(source.getName()).isEqualTo("DebeziumBridgeConfigSource");
    }

    @Test
    void emptyContextProducesEmptyConfigSource() {
        ConfigSourceContext context = createContext(Map.of());

        ConfigSource source = first(factory.getConfigSources(context));

        assertThat(source.getProperties()).isEmpty();
    }

    private ConfigSourceContext createContext(Map<String, String> properties) {
        ConfigSourceContext context = mock(ConfigSourceContext.class);
        when(context.iterateNames()).thenReturn(properties.keySet().iterator());

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            ConfigValue configValue = mock(ConfigValue.class);
            when(configValue.getValue()).thenReturn(entry.getValue());
            when(context.getValue(entry.getKey())).thenReturn(configValue);
        }

        return context;
    }

    private ConfigSource first(Iterable<ConfigSource> sources) {
        return sources.iterator().next();
    }
}
