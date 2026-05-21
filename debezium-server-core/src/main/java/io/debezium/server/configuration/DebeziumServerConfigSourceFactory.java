/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.eclipse.microprofile.config.spi.ConfigSource;

import io.smallrye.config.ConfigSourceContext;
import io.smallrye.config.ConfigSourceFactory;
import io.smallrye.config.ConfigValue;
import io.smallrye.config.common.MapBackedConfigSource;

public class DebeziumServerConfigSourceFactory implements ConfigSourceFactory {
    private static final String DEBEZIUM = "debezium";
    private static final String DEBEZIUM_SOURCE_PREFIX = DEBEZIUM + ".source.";
    private static final String DEBEZIUM_FORMAT_PREFIX = DEBEZIUM + ".format.";
    private static final String QUARKUS_DEBEZIUM_PREFIX = "quarkus." + DEBEZIUM + ".";
    private static final String QUARKUS_DATASOURCE_PREFIX = "quarkus.datasource.";
    private static final String DEBEZIUM_DATASOURCE_PREFIX = DEBEZIUM_SOURCE_PREFIX + "datasource.";

    static final int ORDINAL = 100;

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

            if (name.startsWith(DEBEZIUM_SOURCE_PREFIX)) {
                String suffix = name.substring(DEBEZIUM_SOURCE_PREFIX.length());
                remapped.put(QUARKUS_DEBEZIUM_PREFIX + suffix, value.getValue());

                if (name.startsWith(DEBEZIUM_DATASOURCE_PREFIX)) {
                    String dsSuffix = name.substring(DEBEZIUM_DATASOURCE_PREFIX.length());
                    remapped.put(QUARKUS_DATASOURCE_PREFIX + dsSuffix, value.getValue());
                }
            }
            else if (name.startsWith(QUARKUS_DEBEZIUM_PREFIX)) {
                String suffix = name.substring(QUARKUS_DEBEZIUM_PREFIX.length());
                remapped.put(DEBEZIUM_SOURCE_PREFIX + suffix, value.getValue());
            }

            if (name.startsWith(DEBEZIUM_FORMAT_PREFIX)) {
                if (name.startsWith("debezium.format.schema.registry.url")) {
                    String suffix = name.substring("debezium.format".length());
                    remapped.put("quarkus.debezium.key.converter" + suffix, value.getValue());
                    remapped.put("quarkus.debezium.value.converter" + suffix, value.getValue());
                    remapped.put("quarkus.debezium.header.converter" + suffix, value.getValue());
                }

                String suffix = name.substring(DEBEZIUM.length() + 1);
                remapped.put(QUARKUS_DEBEZIUM_PREFIX + suffix, value.getValue());
            }

            if (name.startsWith(QUARKUS_DATASOURCE_PREFIX)) {
                String dsSuffix = name.substring(QUARKUS_DATASOURCE_PREFIX.length());
                remapped.put(DEBEZIUM_DATASOURCE_PREFIX + dsSuffix, value.getValue());
            }
        }

        if (remapped.isEmpty()) {
            return Collections.emptyList();
        }

        return List.of(new DebeziumServerConfigSource(remapped));
    }

    static class DebeziumServerConfigSource extends MapBackedConfigSource {
        DebeziumServerConfigSource(Map<String, String> properties) {
            super("DebeziumServerConfigSource", properties, ORDINAL);
        }
    }
}
