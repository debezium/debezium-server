/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import io.smallrye.config.ConfigSourceContext;
import io.smallrye.config.ConfigValue;

/**
 * Ported from the original {@code io.debezium.server.DebeziumServerConfigToPropertiesTest} that
 * exercised {@code DebeziumServer.populateEngineProperties}. The configuration mapping logic now
 * lives in {@link DebeziumServerConfigSourceFactory}, so these tests drive the factory directly via
 * a minimal {@link ConfigSourceContext} and assert on the produced {@code quarkus.debezium.*} keys.
 */
class DebeziumServerConfigToPropertiesTest {

    private static final String[] SELECTOR_ARTIFACT_KEYS = {
            "quarkus.debezium.key.converter.key",
            "quarkus.debezium.key.converter.value",
            "quarkus.debezium.key.converter.header",
            "quarkus.debezium.value.converter.key",
            "quarkus.debezium.value.converter.value",
            "quarkus.debezium.value.converter.header",
            "quarkus.debezium.header.converter.key",
            "quarkus.debezium.header.converter.value",
            "quarkus.debezium.header.converter.header"
    };

    @Test
    void genericAppliesWhileGranularOverridesArePreserved() {
        Map<String, String> result = remap(Map.of(
                "debezium.sink.type", "test",
                "debezium.format.schemas.enable", "true",
                "debezium.format.value.schemas.enable", "false",
                "debezium.format.header.schemas.enable", "false"));

        assertThat(result.get("quarkus.debezium.key.converter.schemas.enable")).isEqualTo("true");
        assertThat(result.get("quarkus.debezium.value.converter.schemas.enable")).isEqualTo("false");
        assertThat(result.get("quarkus.debezium.header.converter.schemas.enable")).isEqualTo("false");
    }

    @Test
    void formatSelectorsAreConsumedAndNotWrittenAsConverterProperties() {
        Map<String, String> result = remap(Map.of(
                "debezium.sink.type", "test",
                "debezium.format.key", "json",
                "debezium.format.value", "avro",
                "debezium.format.header", "json",
                "debezium.format.schemas.enable", "true"));

        assertThat(result).doesNotContainKeys("quarkus.debezium.key.converter", "quarkus.debezium.value.converter", "quarkus.debezium.header.converter");
        assertNoSelectorArtifacts(result);
    }

    @Test
    void granularPrefixPropertiesDoNotCrossContaminateOtherConverters() {
        Map<String, String> result = remap(Map.of(
                "debezium.sink.type", "test",
                "debezium.format.key.apicurio.registry.url", "http://key",
                "debezium.format.value.apicurio.registry.url", "http://value",
                "debezium.format.header.apicurio.registry.url", "http://header"));

        assertThat(result.get("quarkus.debezium.key.converter.apicurio.registry.url")).isEqualTo("http://key");
        assertThat(result.get("quarkus.debezium.value.converter.apicurio.registry.url")).isEqualTo("http://value");
        assertThat(result.get("quarkus.debezium.header.converter.apicurio.registry.url")).isEqualTo("http://header");

        assertThat(result).doesNotContainKeys(
                "quarkus.debezium.key.converter.value.apicurio.registry.url",
                "quarkus.debezium.key.converter.header.apicurio.registry.url",
                "quarkus.debezium.value.converter.key.apicurio.registry.url",
                "quarkus.debezium.value.converter.header.apicurio.registry.url",
                "quarkus.debezium.header.converter.key.apicurio.registry.url",
                "quarkus.debezium.header.converter.value.apicurio.registry.url");
    }

    @Test
    void selectorValuesDoNotLeakIntoSyntheticConverterKeyOrValueProperties() {
        Map<String, String> result = remap(Map.of(
                "debezium.sink.type", "test",
                "debezium.format.key", "json",
                "debezium.format.value", "avro",
                "debezium.format.header", "json"));

        // Historical regression guard:
        // old behavior could emit key.converter.key=value selector artifacts.
        assertNoSelectorArtifacts(result);
    }

    @Test
    void shellStyleSelectorValuesDoNotLeakIntoSyntheticConverterKeyOrValueProperties() {
        Map<String, String> result = remap(Map.of(
                "debezium.sink.type", "test",
                "DEBEZIUM_FORMAT_KEY", "json",
                "DEBEZIUM_FORMAT_VALUE", "avro",
                "DEBEZIUM_FORMAT_HEADER", "json"));

        assertThat(result).doesNotContainKeys("quarkus.debezium.key.converter", "quarkus.debezium.value.converter", "quarkus.debezium.header.converter");
        assertNoSelectorArtifacts(result);
    }

    @Test
    @ResourceLock("java.util.Locale")
    void shellStyleSelectorNormalizationIsLocaleIndependent() {
        Locale original = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));

            Map<String, String> result = remap(Map.of(
                    "debezium.sink.type", "test",
                    "debezium.format.schemas.enable", "true",
                    "DEBEZIUM_FORMAT_VALUE_SCHEMAS_ENABLE", "false"));

            assertThat(result.get("quarkus.debezium.key.converter.schemas.enable")).isEqualTo("true");
            assertThat(result.get("quarkus.debezium.value.converter.schemas.enable")).isEqualTo("false");
        }
        finally {
            Locale.setDefault(original);
        }
    }

    @Test
    void microprofileDoubleUnderscoreEscapingIsHandledCorrectly() {
        Map<String, String> result = remap(Map.of(
                "debezium.sink.type", "test",
                // Test that __ (double underscore) encodes a literal underscore, not two dots
                "DEBEZIUM_FORMAT_SCHEMA__REGISTRY__URL", "http://registry"));

        // DEBEZIUM_FORMAT_SCHEMA__REGISTRY__URL should normalize to debezium.format.schema_registry_url
        // not debezium.format.schema..registry..url (which would be incorrect)
        assertThat(result.get("quarkus.debezium.key.converter.schema_registry_url")).isEqualTo("http://registry");
        assertThat(result.get("quarkus.debezium.value.converter.schema_registry_url")).isEqualTo("http://registry");
        assertThat(result.get("quarkus.debezium.header.converter.schema_registry_url")).isEqualTo("http://registry");
    }

    @Test
    void sinkPropertiesRemainAvailableForBothSchemaHistoryAndOffsetStorageMappings() {
        Map<String, String> result = remap(Map.of(
                "debezium.sink.type", "test",
                "debezium.sink.test.url", "http://sink",
                "debezium.sink.test.bucket", "bucket-a"));

        assertThat(result.get("quarkus.debezium.schema.history.internal.test.url")).isEqualTo("http://sink");
        assertThat(result.get("quarkus.debezium.schema.history.internal.test.bucket")).isEqualTo("bucket-a");
        assertThat(result.get("quarkus.debezium.offset.storage.test.url")).isEqualTo("http://sink");
        assertThat(result.get("quarkus.debezium.offset.storage.test.bucket")).isEqualTo("bucket-a");
    }

    private static Map<String, String> remap(Map<String, String> input) {
        Map<String, String> result = new HashMap<>();
        new DebeziumServerConfigSourceFactory().getConfigSources(new MapConfigSourceContext(input))
                .forEach(source -> result.putAll(source.getProperties()));
        return result;
    }

    private static void assertNoSelectorArtifacts(Map<String, String> result) {
        for (String key : SELECTOR_ARTIFACT_KEYS) {
            assertThat(result).doesNotContainKey(key);
        }
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
