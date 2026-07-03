/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.quarkus.bridge;

import static io.smallrye.config.DefaultValuesConfigSource.ORDINAL;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.eclipse.microprofile.config.spi.ConfigSource;

import io.smallrye.config.ConfigSourceContext;
import io.smallrye.config.ConfigSourceFactory;
import io.smallrye.config.ConfigValue;
import io.smallrye.config.common.MapBackedConfigSource;

public class DebeziumBridgeConfigSourceFactory implements ConfigSourceFactory {

    public static final String QUARKUS_DEBEZIUM_SINK = "quarkus.debezium.sink";
    public static final String DEBEZIUM_SINK = "debezium.sink";

    @Override
    public Iterable<ConfigSource> getConfigSources(ConfigSourceContext context) {
        Map<String, String> remapped = new HashMap<>();
        Iterator<String> names = context.iterateNames();

        while (names.hasNext()) {
            String name = names.next();
            ConfigValue value = context.getValue(name);
            if (value == null || value.getValue() == null) {
                continue;
            }
            if (name.startsWith(QUARKUS_DEBEZIUM_SINK)) {
                String suffix = name.substring(QUARKUS_DEBEZIUM_SINK.length());
                remapped.put(DEBEZIUM_SINK + suffix, value.getValue());
            }
        }
        return List.of(new DebeziumBridgeConfigSource(remapped));
    }

    static class DebeziumBridgeConfigSource extends MapBackedConfigSource {
        DebeziumBridgeConfigSource(Map<String, String> properties) {
            super("DebeziumBridgeConfigSource", properties, ORDINAL);
        }
    }
}
