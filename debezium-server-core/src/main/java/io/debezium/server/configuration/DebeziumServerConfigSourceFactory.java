/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.configuration;

import static io.debezium.server.configuration.DebeziumProperties.DEBEZIUM_DATASOURCE_PREFIX;
import static io.debezium.server.configuration.DebeziumProperties.EMPTY_VALUE_SENTINEL;
import static io.debezium.server.configuration.DebeziumProperties.PROP_FORMAT_PREFIX;
import static io.debezium.server.configuration.DebeziumProperties.PROP_HEADER_FORMAT_PREFIX;
import static io.debezium.server.configuration.DebeziumProperties.PROP_KEY_FORMAT_PREFIX;
import static io.debezium.server.configuration.DebeziumProperties.PROP_OFFSET_STORAGE_PREFIX;
import static io.debezium.server.configuration.DebeziumProperties.PROP_PREDICATES;
import static io.debezium.server.configuration.DebeziumProperties.PROP_PREDICATES_PREFIX;
import static io.debezium.server.configuration.DebeziumProperties.PROP_SINK_PREFIX;
import static io.debezium.server.configuration.DebeziumProperties.PROP_SINK_TYPE;
import static io.debezium.server.configuration.DebeziumProperties.PROP_SOURCE_PREFIX;
import static io.debezium.server.configuration.DebeziumProperties.PROP_TRANSFORMS;
import static io.debezium.server.configuration.DebeziumProperties.PROP_TRANSFORMS_PREFIX;
import static io.debezium.server.configuration.DebeziumProperties.PROP_VALUE_FORMAT_PREFIX;
import static io.debezium.server.configuration.DebeziumProperties.QUARKUS_DATASOURCE_PREFIX;
import static io.debezium.server.configuration.DebeziumProperties.QUARKUS_DEBEZIUM_PREFIX;
import static io.debezium.server.configuration.DebeziumProperties.QUARKUS_HEADER_CONVERTER_PREFIX;
import static io.debezium.server.configuration.DebeziumProperties.QUARKUS_KEY_CONVERTER_PREFIX;
import static io.debezium.server.configuration.DebeziumProperties.QUARKUS_VALUE_CONVERTER_PREFIX;

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
 * A {@link ConfigSourceFactory} that bridges Debezium Server's {@code debezium.*} configuration
 * namespace to the Quarkus {@code quarkus.debezium.*} namespace consumed by the Debezium engine
 * extension.
 *
 * <p>This factory performs three main tasks:
 * <ul>
 *   <li><b>Property remapping</b> &mdash; translates {@code debezium.source.*}, {@code debezium.sink.*},
 *       {@code debezium.format.*}, {@code debezium.transforms.*}, and {@code debezium.predicates.*}
 *       properties into their {@code quarkus.debezium.*} equivalents (including converter, schema
 *       history, offset storage, and Apicurio/Schema Registry sub-properties).</li>
 *   <li><b>Datasource bridging</b> &mdash; mirrors {@code debezium.source.datasource.*} properties
 *       to {@code quarkus.datasource.*} and vice versa, so that Debezium Server datasource
 *       configuration is available to Agroal.</li>
 *   <li><b>Empty value preservation</b> &mdash; replaces empty string values with a sentinel
 *       ({@link DebeziumProperties#EMPTY_VALUE_SENTINEL}) so they survive the MicroProfile Config
 *       pipeline, which otherwise treats empty strings as missing. The companion
 *       {@link EmptyStringConverter} converts the sentinel back to an empty string when individual
 *       properties are read.</li>
 * </ul>
 *
 * <p>Shell-style environment variables (e.g. {@code DEBEZIUM_SOURCE_FOO}) are also recognized and
 * normalized to dotted lowercase form before remapping.
 */
public class DebeziumServerConfigSourceFactory implements ConfigSourceFactory {

    private static final Pattern SHELL_PROPERTY_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+_+[a-zA-Z0-9_]+$");

    static final int ORDINAL = 100;

    @Override
    public Iterable<ConfigSource> getConfigSources(ConfigSourceContext context) {
        Map<String, String> remapped = new HashMap<>();

        configToProperties(context, remapped, PROP_SOURCE_PREFIX, QUARKUS_DEBEZIUM_PREFIX, true);
        configToProperties(context, remapped, PROP_FORMAT_PREFIX, QUARKUS_KEY_CONVERTER_PREFIX, true);
        configToProperties(context, remapped, PROP_FORMAT_PREFIX, QUARKUS_VALUE_CONVERTER_PREFIX, true);
        configToProperties(context, remapped, PROP_FORMAT_PREFIX, QUARKUS_HEADER_CONVERTER_PREFIX, true);
        configToProperties(context, remapped, PROP_KEY_FORMAT_PREFIX, QUARKUS_KEY_CONVERTER_PREFIX, true);
        configToProperties(context, remapped, PROP_VALUE_FORMAT_PREFIX, QUARKUS_VALUE_CONVERTER_PREFIX, true);
        configToProperties(context, remapped, PROP_HEADER_FORMAT_PREFIX, QUARKUS_HEADER_CONVERTER_PREFIX, true);
        ConfigValue sink = context.getValue(PROP_SINK_TYPE);
        if (sink != null && sink.getValue() != null) {
            remapped.put(QUARKUS_DEBEZIUM_PREFIX + "name", sink.getValue());
            configToProperties(context, remapped, PROP_SINK_PREFIX + sink.getValue() + ".",
                    QUARKUS_DEBEZIUM_PREFIX + SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + sink.getValue() + ".",
                    false);
            configToProperties(context, remapped, PROP_SINK_PREFIX + sink.getValue() + ".",
                    QUARKUS_DEBEZIUM_PREFIX + PROP_OFFSET_STORAGE_PREFIX + sink.getValue() + ".",
                    false);
        }

        var transforms = context.getValue(PROP_TRANSFORMS);
        if (transforms != null && transforms.getValue() != null) {
            remapped.put(QUARKUS_DEBEZIUM_PREFIX + "transforms", transforms.getValue());
            configToProperties(context, remapped, PROP_TRANSFORMS_PREFIX, QUARKUS_DEBEZIUM_PREFIX + "transforms.", true);
        }

        var predicates = context.getValue(PROP_PREDICATES);
        if (predicates != null && predicates.getValue() != null) {
            remapped.put(QUARKUS_DEBEZIUM_PREFIX + "predicates", predicates.getValue());
            configToProperties(context, remapped, PROP_PREDICATES_PREFIX, QUARKUS_DEBEZIUM_PREFIX + "predicates.", true);
        }

        Iterator<String> names = context.iterateNames();
        while (names.hasNext()) {
            String name = names.next();
            ConfigValue value = context.getValue(name);
            if (value == null || value.getValue() == null) {
                continue;
            }

            if (name.startsWith(PROP_SOURCE_PREFIX)) {
                String suffix = name.substring(PROP_SOURCE_PREFIX.length());
                remapped.put(QUARKUS_DEBEZIUM_PREFIX + suffix, value.getValue());

                if (name.startsWith(DEBEZIUM_DATASOURCE_PREFIX)) {
                    String dsSuffix = name.substring(DEBEZIUM_DATASOURCE_PREFIX.length());
                    remapped.put(QUARKUS_DATASOURCE_PREFIX + dsSuffix, value.getValue());
                }
            }
            else if (name.startsWith(QUARKUS_DEBEZIUM_PREFIX)) {
                String suffix = name.substring(QUARKUS_DEBEZIUM_PREFIX.length());
                remapped.put(PROP_SOURCE_PREFIX + suffix, value.getValue());
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
