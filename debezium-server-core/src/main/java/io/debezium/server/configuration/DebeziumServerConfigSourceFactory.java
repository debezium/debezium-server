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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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

        // Get property names as a mutable set to remove properties as they get processed, avoiding duplication
        Set<String> remainingPropertyNames = new LinkedHashSet<>();
        Map<String, String> normalizedNames = new HashMap<>();
        Iterator<String> allNames = context.iterateNames();
        while (allNames.hasNext()) {
            String propName = allNames.next();
            remainingPropertyNames.add(propName);
            normalizedNames.put(propName, normalizePropertyName(propName));
        }

        // Handle source properties first
        configToProperties(context, remapped,
                new ConfigToPropertiesMapping(PROP_SOURCE_PREFIX, QUARKUS_DEBEZIUM_PREFIX, remainingPropertyNames, normalizedNames, true, true));

        // Handle granular (debezium.format.key|value|header.*) props first and remove from the potential names
        configToProperties(context, remapped,
                new ConfigToPropertiesMapping(PROP_KEY_FORMAT_PREFIX, QUARKUS_KEY_CONVERTER_PREFIX, remainingPropertyNames, normalizedNames, true, true));
        configToProperties(context, remapped,
                new ConfigToPropertiesMapping(PROP_VALUE_FORMAT_PREFIX, QUARKUS_VALUE_CONVERTER_PREFIX, remainingPropertyNames, normalizedNames, true, true));
        configToProperties(context, remapped,
                new ConfigToPropertiesMapping(PROP_HEADER_FORMAT_PREFIX, QUARKUS_HEADER_CONVERTER_PREFIX, remainingPropertyNames, normalizedNames, true, true));

        // Remove the format-selector properties (debezium.format.key/value/header = avro|json|...) so that
        // the generic debezium.format.* pass below does not propagate them as nonsensical converter
        // sub-properties (e.g. key.converter.key = avro, header.converter.value = avro).
        // Their values have already been consumed above via getFormat() / getHeaderFormat() in the engine.
        removePropertyName(remainingPropertyNames, normalizedNames, DebeziumProperties.PROP_KEY_FORMAT);
        removePropertyName(remainingPropertyNames, normalizedNames, DebeziumProperties.PROP_VALUE_FORMAT);
        removePropertyName(remainingPropertyNames, normalizedNames, DebeziumProperties.PROP_HEADER_FORMAT);

        // Handle the remaining generic (debezium.format.*) props. Don't remove them so that they can apply to key, value and header
        configToProperties(context, remapped,
                new ConfigToPropertiesMapping(PROP_FORMAT_PREFIX, QUARKUS_KEY_CONVERTER_PREFIX, remainingPropertyNames, normalizedNames, false, false));
        configToProperties(context, remapped,
                new ConfigToPropertiesMapping(PROP_FORMAT_PREFIX, QUARKUS_VALUE_CONVERTER_PREFIX, remainingPropertyNames, normalizedNames, false, false));
        configToProperties(context, remapped,
                new ConfigToPropertiesMapping(PROP_FORMAT_PREFIX, QUARKUS_HEADER_CONVERTER_PREFIX, remainingPropertyNames, normalizedNames, false, false));

        ConfigValue sink = context.getValue(PROP_SINK_TYPE);
        if (sink != null && sink.getValue() != null) {
            remapped.put(QUARKUS_DEBEZIUM_PREFIX + "name", sink.getValue());
            String sinkPrefix = PROP_SINK_PREFIX + sink.getValue() + ".";

            // The sink connection properties are reused for the schema history and offset storage
            // namespaces as a convenience when the same technology is used for both the sink and the
            // storage. This is only done when the corresponding storage namespace has not been
            // configured explicitly; otherwise the copied sink properties could silently override the
            // user's explicit storage configuration (e.g. when the sink and storage use different
            // property names for the same concept) or leak sink-specific properties into the storage
            // namespaces where they have no meaning.
            String schemaHistoryPrefix = QUARKUS_DEBEZIUM_PREFIX + SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + sink.getValue() + ".";
            if (!hasPropertyWithPrefix(context, remapped, schemaHistoryPrefix)) {
                configToProperties(context, remapped,
                        new ConfigToPropertiesMapping(sinkPrefix, schemaHistoryPrefix, remainingPropertyNames, normalizedNames, false, false));
            }

            String offsetStoragePrefix = QUARKUS_DEBEZIUM_PREFIX + PROP_OFFSET_STORAGE_PREFIX + sink.getValue() + ".";
            if (!hasPropertyWithPrefix(context, remapped, offsetStoragePrefix)) {
                configToProperties(context, remapped,
                        new ConfigToPropertiesMapping(sinkPrefix, offsetStoragePrefix, remainingPropertyNames, normalizedNames, false, true));
            }
        }

        var transforms = context.getValue(PROP_TRANSFORMS);
        if (transforms != null && transforms.getValue() != null) {
            remapped.put(QUARKUS_DEBEZIUM_PREFIX + "transforms", transforms.getValue());
            configToProperties(context, remapped,
                    new ConfigToPropertiesMapping(PROP_TRANSFORMS_PREFIX, QUARKUS_DEBEZIUM_PREFIX + "transforms.", remainingPropertyNames, normalizedNames, true, true));
        }

        var predicates = context.getValue(PROP_PREDICATES);
        if (predicates != null && predicates.getValue() != null) {
            remapped.put(QUARKUS_DEBEZIUM_PREFIX + "predicates", predicates.getValue());
            configToProperties(context, remapped,
                    new ConfigToPropertiesMapping(PROP_PREDICATES_PREFIX, QUARKUS_DEBEZIUM_PREFIX + "predicates.", remainingPropertyNames, normalizedNames, true, true));
        }

        // The remaining loop handles datasource bridging and the reverse mapping (quarkus.debezium.* -> debezium.source.*)
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

    /**
     * Encapsulates property mapping configuration with mutable state (property names and normalized names cache).
     * This is not a pure value object since it holds mutable collections that are modified during iteration.
     */
    private static final class ConfigToPropertiesMapping {
        final String oldPrefix;
        final String newPrefix;
        final Set<String> propertyNames;
        final Map<String, String> normalizedNames;
        final boolean overwrite;
        final boolean removeProcessedPropertyNames;

        private ConfigToPropertiesMapping(String oldPrefix, String newPrefix, Set<String> propertyNames,
                                          Map<String, String> normalizedNames, boolean overwrite, boolean removeProcessedPropertyNames) {
            this.oldPrefix = oldPrefix;
            this.newPrefix = newPrefix;
            this.propertyNames = propertyNames;
            this.normalizedNames = normalizedNames;
            this.overwrite = overwrite;
            this.removeProcessedPropertyNames = removeProcessedPropertyNames;
        }

    }

    private void configToProperties(ConfigSourceContext context, Map<String, String> mutableMap, ConfigToPropertiesMapping mapping) {

        // Use iterator to safely remove items while iterating
        Iterator<String> iterator = mapping.propertyNames.iterator();
        while (iterator.hasNext()) {
            String name = iterator.next();
            boolean processed = false;

            String normalizedName = mapping.normalizedNames.get(name);
            if (normalizedName != null && normalizedName.startsWith(mapping.oldPrefix)) {
                String finalPropertyName = mapping.newPrefix + normalizedName.substring(mapping.oldPrefix.length());
                if (mapping.overwrite || !mutableMap.containsKey(finalPropertyName)) {
                    mutableMap.put(finalPropertyName, resolvePropertyValue(context, name, normalizedName));
                }
                processed = true;
            }
            else if (name.startsWith(mapping.oldPrefix)) {
                String finalPropertyName = mapping.newPrefix + name.substring(mapping.oldPrefix.length());
                if (mapping.overwrite || !mutableMap.containsKey(finalPropertyName)) {
                    mutableMap.put(finalPropertyName, resolvePropertyValue(context, name, normalizedName));
                }
                processed = true;
            }

            // Remove processed properties to avoid duplicate processing
            if (processed && mapping.removeProcessedPropertyNames) {
                iterator.remove();
            }
        }
    }

    private void removePropertyName(Set<String> propertyNames, Map<String, String> normalizedNames, String propertyName) {
        propertyNames.removeIf(name -> propertyName.equals(normalizedNames.get(name)));
    }

    private String resolvePropertyValue(ConfigSourceContext context, String originalName, String normalizedName) {
        // Prefer canonical normalized lookup so source precedence is handled by config resolution.
        if (normalizedName != null) {
            ConfigValue normalizedValue = context.getValue(normalizedName);
            if (normalizedValue != null && normalizedValue.getValue() != null) {
                return normalizedValue.getValue();
            }
        }

        ConfigValue originalValue = context.getValue(originalName);
        if (originalValue != null && originalValue.getValue() != null) {
            return originalValue.getValue();
        }

        // Fall back to the raw value (possibly null) so missing values fail loudly rather than defaulting to "".
        return originalValue != null ? originalValue.getValue() : null;
    }

    private String normalizePropertyName(String name) {
        if (SHELL_PROPERTY_NAME_PATTERN.matcher(name).matches()) {
            // Handle MicroProfile escaping: __ encodes a literal underscore, _ encodes a dot
            StringBuilder normalized = new StringBuilder(name.length());
            int i = 0;
            while (i < name.length()) {
                if (i + 1 < name.length() && name.charAt(i) == '_' && name.charAt(i + 1) == '_') {
                    // Double underscore → literal underscore
                    normalized.append('_');
                    i += 2;
                }
                else if (name.charAt(i) == '_') {
                    // Single underscore → dot
                    normalized.append('.');
                    i++;
                }
                else {
                    normalized.append(name.charAt(i));
                    i++;
                }
            }
            return normalized.toString().toLowerCase(Locale.ROOT);
        }
        return name;
    }

    private static boolean hasPropertyWithPrefix(ConfigSourceContext context, Map<String, String> properties, String prefix) {
        if (properties.keySet().stream().anyMatch(name -> name.startsWith(prefix))) {
            return true;
        }

        Iterator<String> names = context.iterateNames();
        while (names.hasNext()) {
            String name = names.next();
            ConfigValue value = context.getValue(name);
            if (value != null && value.getValue() != null && name.startsWith(prefix)) {
                return true;
            }
        }

        return false;
    }

    static class DebeziumServerConfigSource extends MapBackedConfigSource {
        DebeziumServerConfigSource(Map<String, String> properties) {
            super("DebeziumServerConfigSource", properties, ORDINAL);
        }
    }
}
