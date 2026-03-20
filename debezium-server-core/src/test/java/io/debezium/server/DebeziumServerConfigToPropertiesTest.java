/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import io.debezium.relational.history.SchemaHistory;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;

class DebeziumServerConfigToPropertiesTest {

    private static final String[] SELECTOR_ARTIFACT_KEYS = {
            "key.converter.key",
            "key.converter.value",
            "key.converter.header",
            "value.converter.key",
            "value.converter.value",
            "value.converter.header",
            "header.converter.key",
            "header.converter.value",
            "header.converter.header"
    };

    @Test
    void genericAppliesWhileGranularOverridesArePreserved() throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("debezium.sink.type", "test");
        values.put("debezium.format.schemas.enable", "true");
        values.put("debezium.format.value.schemas.enable", "false");
        values.put("debezium.format.header.schemas.enable", "false");

        Properties props = mapProperties(values);

        assertThat(props.getProperty("key.converter.schemas.enable")).isEqualTo("true");
        assertThat(props.getProperty("value.converter.schemas.enable")).isEqualTo("false");
        assertThat(props.getProperty("header.converter.schemas.enable")).isEqualTo("false");
    }

    @Test
    void formatSelectorsAreConsumedAndNotWrittenAsConverterProperties() throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("debezium.sink.type", "test");
        values.put("debezium.format.key", "json");
        values.put("debezium.format.value", "avro");
        values.put("debezium.format.header", "json");
        values.put("debezium.format.schemas.enable", "true");

        Properties props = mapProperties(values);

        assertThat(props).doesNotContainKeys("key.converter", "value.converter", "header.converter");
        assertNoSelectorArtifacts(props);
    }

    @Test
    void granularPrefixPropertiesDoNotCrossContaminateOtherConverters() throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("debezium.sink.type", "test");
        values.put("debezium.format.key.apicurio.registry.url", "http://key");
        values.put("debezium.format.value.apicurio.registry.url", "http://value");
        values.put("debezium.format.header.apicurio.registry.url", "http://header");

        Properties props = mapProperties(values);

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
        values.put("debezium.sink.type", "test");
        values.put("debezium.format.key", "json");
        values.put("debezium.format.value", "avro");
        values.put("debezium.format.header", "json");

        Properties props = mapProperties(values);

        // Historical regression guard:
        // old behavior could emit key.converter.key=value selector artifacts.
        assertNoSelectorArtifacts(props);
    }

    @Test
    void shellStyleSelectorValuesDoNotLeakIntoSyntheticConverterKeyOrValueProperties() throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("debezium.sink.type", "test");
        values.put("DEBEZIUM_FORMAT_KEY", "json");
        values.put("DEBEZIUM_FORMAT_VALUE", "avro");
        values.put("DEBEZIUM_FORMAT_HEADER", "json");

        Properties props = mapProperties(values);

        assertThat(props).doesNotContainKeys("key.converter", "value.converter", "header.converter");
        assertNoSelectorArtifacts(props);
    }

    @Test
    @ResourceLock("java.util.Locale")
    void shellStyleSelectorNormalizationIsLocaleIndependent() {
        Locale original = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));

            Map<String, String> values = new HashMap<>();
            values.put("debezium.sink.type", "test");
            values.put("debezium.format.schemas.enable", "true");
            values.put("DEBEZIUM_FORMAT_VALUE_SCHEMAS_ENABLE", "false");

            Properties props = mapProperties(values);

            assertThat(props.getProperty("key.converter.schemas.enable")).isEqualTo("true");
            assertThat(props.getProperty("value.converter.schemas.enable")).isEqualTo("false");
        }
        finally {
            Locale.setDefault(original);
        }
    }

    @Test
    void microprofileDoubleUnderscoreEscapingIsHandledCorrectly() throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("debezium.sink.type", "test");
        // Test that __ (double underscore) encodes a literal underscore, not two dots
        values.put("DEBEZIUM_FORMAT_SCHEMA__REGISTRY__URL", "http://registry");

        Properties props = mapProperties(values);

        // DEBEZIUM_FORMAT_SCHEMA__REGISTRY__URL should normalize to debezium.format.schema_registry_url
        // not debezium.format.schema..registry..url (which would be incorrect)
        assertThat(props.getProperty("key.converter.schema_registry_url")).isEqualTo("http://registry");
        assertThat(props.getProperty("value.converter.schema_registry_url")).isEqualTo("http://registry");
        assertThat(props.getProperty("header.converter.schema_registry_url")).isEqualTo("http://registry");
    }

    @Test
    void sinkPropertiesRemainAvailableForBothSchemaHistoryAndOffsetStorageMappings() throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("debezium.sink.type", "test");
        values.put("debezium.sink.test.url", "http://sink");
        values.put("debezium.sink.test.bucket", "bucket-a");

        Properties props = mapProperties(values);

        assertThat(props.getProperty(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + "test.url")).isEqualTo("http://sink");
        assertThat(props.getProperty(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + "test.bucket")).isEqualTo("bucket-a");
        assertThat(props.getProperty("offset.storage.test.url")).isEqualTo("http://sink");
        assertThat(props.getProperty("offset.storage.test.bucket")).isEqualTo("bucket-a");
    }

    private static Properties mapProperties(Map<String, String> values) {
        DebeziumServer server = new DebeziumServer();
        Properties props = new Properties();
        Config config = new SmallRyeConfigBuilder()
                .withSources(new PropertiesConfigSource(values, "test"))
                .build();
        server.populateEngineProperties(config, "test", props);
        return props;
    }

    private static void assertNoSelectorArtifacts(Properties props) {
        assertThat(props).doesNotContainKeys((Object[]) SELECTOR_ARTIFACT_KEYS);
    }
}
