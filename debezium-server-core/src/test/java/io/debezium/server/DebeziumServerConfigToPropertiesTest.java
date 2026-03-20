/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigValue;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.config.spi.Converter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.debezium.relational.history.SchemaHistory;

class DebeziumServerConfigToPropertiesTest {

    private static final String PROP_FORMAT_PREFIX = "debezium.format.";
    private static final String PROP_KEY_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "key.";
    private static final String PROP_VALUE_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "value.";
    private static final String PROP_HEADER_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "header.";

    private static final String PROP_KEY_FORMAT = PROP_FORMAT_PREFIX + "key";
    private static final String PROP_VALUE_FORMAT = PROP_FORMAT_PREFIX + "value";
    private static final String PROP_HEADER_FORMAT = PROP_FORMAT_PREFIX + "header";
    private static final String PROP_SINK_PREFIX = "debezium.sink.";
    private static final String PROP_OFFSET_STORAGE_PREFIX = "offset.storage.";

    private static Method configToPropertiesMethod;

    @BeforeAll
    static void locateMethod() throws Exception {
        configToPropertiesMethod = DebeziumServer.class.getDeclaredMethod(
                "configToProperties",
                Config.class,
                Properties.class,
                String.class,
                String.class,
                boolean.class,
                Set.class,
                boolean.class);
        configToPropertiesMethod.setAccessible(true);
    }

    @Test
    void genericAppliesWhileGranularOverridesArePreserved() throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("debezium.format.schemas.enable", "true");
        values.put("debezium.format.value.schemas.enable", "false");
        values.put("debezium.format.header.schemas.enable", "false");

        Properties props = applyFormatPropertyMapping(values);

        assertThat(props.getProperty("key.converter.schemas.enable")).isEqualTo("true");
        assertThat(props.getProperty("value.converter.schemas.enable")).isEqualTo("false");
        assertThat(props.getProperty("header.converter.schemas.enable")).isEqualTo("false");
    }

    @Test
    void formatSelectorsAreConsumedAndNotWrittenAsConverterProperties() throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("debezium.format.key", "json");
        values.put("debezium.format.value", "avro");
        values.put("debezium.format.header", "json");
        values.put("debezium.format.schemas.enable", "true");

        Properties props = applyFormatPropertyMapping(values);

        assertThat(props).doesNotContainKeys("key.converter", "value.converter", "header.converter");
        assertThat(props).doesNotContainKeys("key.converter.key", "value.converter.value", "header.converter.header");
    }

    @Test
    void granularPrefixPropertiesDoNotCrossContaminateOtherConverters() throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("debezium.format.key.apicurio.registry.url", "http://key");
        values.put("debezium.format.value.apicurio.registry.url", "http://value");
        values.put("debezium.format.header.apicurio.registry.url", "http://header");

        Properties props = applyFormatPropertyMapping(values);

        assertThat(props.getProperty("key.converter.apicurio.registry.url")).isEqualTo("http://key");
        assertThat(props.getProperty("value.converter.apicurio.registry.url")).isEqualTo("http://value");
        assertThat(props.getProperty("header.converter.apicurio.registry.url")).isEqualTo("http://header");

        assertThat(props).doesNotContainKeys(
                "key.converter.value.apicurio.registry.url",
                "key.converter.header.apicurio.registry.url",
                "value.converter.key.apicurio.registry.url",
                "value.converter.header.apicurio.registry.url",
                "header.converter.key.apicurio.registry.url",
                "header.converter.value.apicurio.registry.url");
    }

    @Test
    void selectorValuesDoNotLeakIntoSyntheticConverterKeyOrValueProperties() throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("debezium.format.key", "json");
        values.put("debezium.format.value", "avro");
        values.put("debezium.format.header", "json");

        Properties props = applyFormatPropertyMapping(values);

        // Historical regression guard:
        // old behavior could emit key.converter.key=value selector artifacts.
        assertThat(props).doesNotContainKeys(
                "key.converter.key",
                "key.converter.value",
                "key.converter.header",
                "value.converter.key",
                "value.converter.value",
                "value.converter.header",
                "header.converter.key",
                "header.converter.value",
                "header.converter.header");
    }

    @Test
    void sinkPropertiesRemainAvailableForBothSchemaHistoryAndOffsetStorageMappings() throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("debezium.sink.test.url", "http://sink");
        values.put("debezium.sink.test.bucket", "bucket-a");

        Properties props = applySinkPropertyMapping(values, "test");

        assertThat(props.getProperty(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + "test.url")).isEqualTo("http://sink");
        assertThat(props.getProperty(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + "test.bucket")).isEqualTo("bucket-a");
        assertThat(props.getProperty("offset.storage.test.url")).isEqualTo("http://sink");
        assertThat(props.getProperty("offset.storage.test.bucket")).isEqualTo("bucket-a");
    }

    private static Properties applyFormatPropertyMapping(Map<String, String> values) throws Exception {
        DebeziumServer server = new DebeziumServer();
        Properties props = new Properties();
        Config config = new MapBackedConfig(values);
        Set<String> remainingPropertyNames = new HashSet<>(values.keySet());

        invoke(server, config, props, PROP_KEY_FORMAT_PREFIX, "key.converter.", true, remainingPropertyNames, true);
        invoke(server, config, props, PROP_VALUE_FORMAT_PREFIX, "value.converter.", true, remainingPropertyNames, true);
        invoke(server, config, props, PROP_HEADER_FORMAT_PREFIX, "header.converter.", true, remainingPropertyNames, true);

        // Matches DebeziumServer.start(): format selector values are consumed by getFormat/getHeaderFormat
        // and should not be re-written as converter properties.
        remainingPropertyNames.remove(PROP_KEY_FORMAT);
        remainingPropertyNames.remove(PROP_VALUE_FORMAT);
        remainingPropertyNames.remove(PROP_HEADER_FORMAT);

        invoke(server, config, props, PROP_FORMAT_PREFIX, "key.converter.", false, remainingPropertyNames, false);
        invoke(server, config, props, PROP_FORMAT_PREFIX, "value.converter.", false, remainingPropertyNames, false);
        invoke(server, config, props, PROP_FORMAT_PREFIX, "header.converter.", false, remainingPropertyNames, false);

        return props;
    }

    private static Properties applySinkPropertyMapping(Map<String, String> values, String sinkName) throws Exception {
        DebeziumServer server = new DebeziumServer();
        Properties props = new Properties();
        Config config = new MapBackedConfig(values);
        Set<String> remainingPropertyNames = new HashSet<>(values.keySet());

        invoke(server, config, props, PROP_SINK_PREFIX + sinkName + ".",
                SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + sinkName + ".",
                false, new HashSet<>(remainingPropertyNames), true);
        invoke(server, config, props, PROP_SINK_PREFIX + sinkName + ".", PROP_OFFSET_STORAGE_PREFIX + sinkName + ".",
                false, remainingPropertyNames, true);

        return props;
    }

    private static void invoke(DebeziumServer server, Config config, Properties props, String oldPrefix, String newPrefix,
                               boolean overwrite, Set<String> propertyNames, boolean removeName)
            throws Exception {
        configToPropertiesMethod.invoke(server, config, props, oldPrefix, newPrefix, overwrite, propertyNames, removeName);
    }

    private static class MapBackedConfig implements Config {

        private final Map<String, String> values;

        private MapBackedConfig(Map<String, String> values) {
            this.values = values;
        }

        @Override
        public <T> T getValue(String propertyName, Class<T> propertyType) {
            String value = values.get(propertyName);
            if (value == null) {
                throw new IllegalArgumentException("No such property: " + propertyName);
            }
            return convert(value, propertyType);
        }

        @Override
        public ConfigValue getConfigValue(String propertyName) {
            String value = values.get(propertyName);
            return new BasicConfigValue(propertyName, value);
        }

        @Override
        public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
            String value = values.get(propertyName);
            if (value == null) {
                return Optional.empty();
            }
            return Optional.of(convert(value, propertyType));
        }

        @Override
        public Iterable<String> getPropertyNames() {
            return values.keySet();
        }

        @Override
        public Iterable<ConfigSource> getConfigSources() {
            return new ArrayList<>();
        }

        @Override
        public <T> Optional<Converter<T>> getConverter(Class<T> forType) {
            return Optional.empty();
        }

        @Override
        public <T> T unwrap(Class<T> type) {
            throw new IllegalArgumentException("Unsupported unwrap type: " + type);
        }

        @SuppressWarnings("unchecked")
        private <T> T convert(String value, Class<T> type) {
            if (String.class.equals(type)) {
                return (T) value;
            }
            throw new IllegalArgumentException("Unsupported property type: " + type);
        }
    }

    private static class BasicConfigValue implements ConfigValue {

        private final String name;
        private final String value;

        private BasicConfigValue(String name, String value) {
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public String getRawValue() {
            return value;
        }

        @Override
        public String getSourceName() {
            return "map";
        }

        @Override
        public int getSourceOrdinal() {
            return 1000;
        }
    }
}
