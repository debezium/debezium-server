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
import java.util.regex.Pattern;

import org.eclipse.microprofile.config.spi.ConfigSource;

import io.debezium.relational.history.SchemaHistory;
import io.smallrye.config.ConfigSourceContext;
import io.smallrye.config.ConfigSourceFactory;
import io.smallrye.config.ConfigValue;
import io.smallrye.config.common.MapBackedConfigSource;

/**
 * TODO: to refactor completely. seek and fix but this file should address the same things:
 * reference: https://github.com/debezium/debezium-server/blob/main/debezium-server-core/src/main/java/io/debezium/server/DebeziumServer.java
 * ```java
 *
 *     private static final String PROP_PREFIX = "debezium.";
 *     static final String PROP_SOURCE_PREFIX = PROP_PREFIX + "source.";
 *     private static final String PROP_SINK_PREFIX = PROP_PREFIX + "sink.";
 *     private static final String PROP_FORMAT_PREFIX = PROP_PREFIX + "format.";
 *     private static final String PROP_PREDICATES_PREFIX = PROP_PREFIX + "predicates.";
 *     private static final String PROP_TRANSFORMS_PREFIX = PROP_PREFIX + "transforms.";
 *     private static final String PROP_HEADER_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "header.";
 *     private static final String PROP_KEY_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "key.";
 *     private static final String PROP_VALUE_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "value.";
 *     private static final String PROP_OFFSET_STORAGE_PREFIX = "offset.storage.";
 *
 *     private static final String PROP_PREDICATES = PROP_PREFIX + "predicates";
 *     private static final String PROP_TRANSFORMS = PROP_PREFIX + "transforms";
 *     static final String PROP_SINK_TYPE = PROP_SINK_PREFIX + "type";
 *
 *     private static final String PROP_HEADER_FORMAT = PROP_FORMAT_PREFIX + "header";
 *     private static final String PROP_KEY_FORMAT = PROP_FORMAT_PREFIX + "key";
 *     private static final String PROP_VALUE_FORMAT = PROP_FORMAT_PREFIX + "value";
 *     private static final String PROP_TERMINATION_WAIT = PROP_PREFIX + "termination.wait";
 *
 *         configToProperties(config, props, PROP_SOURCE_PREFIX, "", true);
 *         configToProperties(config, props, PROP_FORMAT_PREFIX, "key.converter.", true);
 *         configToProperties(config, props, PROP_FORMAT_PREFIX, "value.converter.", true);
 *         configToProperties(config, props, PROP_FORMAT_PREFIX, "header.converter.", true);
 *         configToProperties(config, props, PROP_KEY_FORMAT_PREFIX, "key.converter.", true);
 *         configToProperties(config, props, PROP_VALUE_FORMAT_PREFIX, "value.converter.", true);
 *         configToProperties(config, props, PROP_HEADER_FORMAT_PREFIX, "header.converter.", true);
 *         configToProperties(config, props, PROP_SINK_PREFIX + name + ".", SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + name + ".", false);
 *         configToProperties(config, props, PROP_SINK_PREFIX + name + ".", PROP_OFFSET_STORAGE_PREFIX + name + ".", false);
 *
 *
 *   private void configToProperties(Config config, Properties props, String oldPrefix, String newPrefix, boolean overwrite) {
 *       for (String name : config.getPropertyNames()) {
 *           String updatedPropertyName = null;
 *           if (SHELL_PROPERTY_NAME_PATTERN.matcher(name).matches()) {
 *               updatedPropertyName = name.replace("_", ".").toLowerCase();
 *           }
 *           if (updatedPropertyName != null && updatedPropertyName.startsWith(oldPrefix)) {
 *               String finalPropertyName = newPrefix + updatedPropertyName.substring(oldPrefix.length());
 *               if (overwrite || !props.containsKey(finalPropertyName)) {
 *                   props.setProperty(finalPropertyName, config.getOptionalValue(name, String.class).orElse(""));
 *               }
 *           }
 *           else if (name.startsWith(oldPrefix)) {
 *               String finalPropertyName = newPrefix + name.substring(oldPrefix.length());
 *               if (overwrite || !props.containsKey(finalPropertyName)) {
 *                   props.setProperty(finalPropertyName, config.getConfigValue(name).getValue());
 *               }
 *           }
 *       }
 *   }
 * ```
 */
public class DebeziumServerConfigSourceFactory implements ConfigSourceFactory {
    private static final String DEBEZIUM = "debezium";
    private static final String DEBEZIUM_SOURCE_PREFIX = DEBEZIUM + ".source.";
    private static final String DEBEZIUM_FORMAT_PREFIX = DEBEZIUM + ".format.";
    private static final String QUARKUS_DEBEZIUM_PREFIX = "quarkus." + DEBEZIUM + ".";
    private static final String QUARKUS_DATASOURCE_PREFIX = "quarkus.datasource.";
    private static final String DEBEZIUM_DATASOURCE_PREFIX = DEBEZIUM_SOURCE_PREFIX + "datasource.";

    private static final Pattern SHELL_PROPERTY_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+_+[a-zA-Z0-9_]+$");

    private static final String PROP_PREFIX = "debezium.";
    static final String PROP_SOURCE_PREFIX = PROP_PREFIX + "source.";
    private static final String PROP_SINK_PREFIX = PROP_PREFIX + "sink.";
    private static final String PROP_FORMAT_PREFIX = PROP_PREFIX + "format.";
    private static final String PROP_PREDICATES_PREFIX = PROP_PREFIX + "predicates.";
    private static final String PROP_TRANSFORMS_PREFIX = PROP_PREFIX + "transforms.";
    private static final String PROP_HEADER_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "header.";
    private static final String PROP_KEY_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "key.";
    private static final String PROP_VALUE_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "value.";
    private static final String PROP_OFFSET_STORAGE_PREFIX = "offset.storage.";

    private static final String PROP_PREDICATES = PROP_PREFIX + "predicates";
    private static final String PROP_TRANSFORMS = PROP_PREFIX + "transforms";
    static final String PROP_SINK_TYPE = PROP_SINK_PREFIX + "type";

    /**
     * TODO: evaluate the impact
     */
    private static final String PROP_ENGINE_FACTORY = PROP_PREFIX + "engine.factory";

    static final String EMPTY_VALUE_SENTINEL = "__DBZ_EMPTY__";

    static final int ORDINAL = 100;

    @Override
    public Iterable<ConfigSource> getConfigSources(ConfigSourceContext context) {
        Map<String, String> remapped = new HashMap<>();

        configToProperties(context, remapped, PROP_SOURCE_PREFIX, "quarkus.debezium.", true);
        configToProperties(context, remapped, PROP_FORMAT_PREFIX, "quarkus.debezium.key.converter.", true);
        configToProperties(context, remapped, PROP_FORMAT_PREFIX, "quarkus.debezium.value.converter.", true);
        configToProperties(context, remapped, PROP_FORMAT_PREFIX, "quarkus.debezium.header.converter.", true);
        configToProperties(context, remapped, PROP_KEY_FORMAT_PREFIX, "quarkus.debezium.key.converter.", true);
        configToProperties(context, remapped, PROP_VALUE_FORMAT_PREFIX, "quarkus.debezium.value.converter.", true);
        configToProperties(context, remapped, PROP_HEADER_FORMAT_PREFIX, "quarkus.debezium.header.converter.", true);
        ConfigValue sink = context.getValue(PROP_SINK_TYPE);
        if (sink != null && sink.getValue() != null) {
            remapped.put("quarkus.debezium.name", sink.getValue());
            configToProperties(context, remapped, PROP_SINK_PREFIX + sink.getValue() + ".",
                    "quarkus.debezium." + SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + sink.getValue() + ".",
                    false);
            configToProperties(context, remapped, PROP_SINK_PREFIX + sink.getValue() + ".", "quarkus.debezium." + PROP_OFFSET_STORAGE_PREFIX + sink.getValue() + ".",
                    false);
        }

        var transforms = context.getValue(PROP_TRANSFORMS);
        if (transforms != null && transforms.getValue() != null) {
            remapped.put("quarkus.debezium.transforms", transforms.getValue());
            configToProperties(context, remapped, PROP_TRANSFORMS_PREFIX, "quarkus.debezium.transforms.", true);
        }

        var predicates = context.getValue(PROP_PREDICATES);
        if (predicates != null && predicates.getValue() != null) {
            remapped.put("quarkus.debezium.predicates", predicates.getValue());
            configToProperties(context, remapped, PROP_PREDICATES_PREFIX, "quarkus.debezium.predicates.", true);
        }

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

                if (name.startsWith("debezium.format.key.apicurio.registry.")) {
                    String suffix = name.substring("debezium.format.key".length());
                    remapped.put("quarkus.debezium.key.converter" + suffix, value.getValue());
                }
                else if (name.startsWith("debezium.format.value.apicurio.registry.")) {
                    String suffix = name.substring("debezium.format.value".length());
                    remapped.put("quarkus.debezium.value.converter" + suffix, value.getValue());
                }
                else if (name.startsWith("debezium.format.header.apicurio.registry.")) {
                    String suffix = name.substring("debezium.format.header".length());
                    remapped.put("quarkus.debezium.header.converter" + suffix, value.getValue());
                }
                else if (name.startsWith("debezium.format.apicurio.registry.")) {
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

        remapped.replaceAll((k, v) -> v != null && v.isEmpty() ? EMPTY_VALUE_SENTINEL : v);

        return List.of(new DebeziumServerConfigSource(remapped));
    }

    private void configToProperties(ConfigSourceContext context, Map<String, String> mutableMap, String oldPrefix, String newPrefix, boolean overwrite) {
        context.iterateNames().forEachRemaining(name -> {
            String updatedPropertyName = null;

            if (SHELL_PROPERTY_NAME_PATTERN.matcher(name).matches()) {
                updatedPropertyName = name.replace("_", ".").toLowerCase();
            }

            if (updatedPropertyName != null && updatedPropertyName.startsWith(oldPrefix)) {
                String finalPropertyName = newPrefix + updatedPropertyName.substring(oldPrefix.length());
                if (overwrite || !mutableMap.containsKey(finalPropertyName)) {
                    mutableMap.put(finalPropertyName, context.getValue(name).getValueOrDefault(""));
                }
            }
            else if (name.startsWith(oldPrefix)) {
                String finalPropertyName = newPrefix + name.substring(oldPrefix.length());
                if (overwrite || !mutableMap.containsKey(finalPropertyName)) {
                    mutableMap.put(finalPropertyName, context.getValue(name).getValue());
                }
            }
        });
    }

    static class DebeziumServerConfigSource extends MapBackedConfigSource {
        DebeziumServerConfigSource(Map<String, String> properties) {
            super("DebeziumServerConfigSource", properties, ORDINAL);
        }
    }
}
