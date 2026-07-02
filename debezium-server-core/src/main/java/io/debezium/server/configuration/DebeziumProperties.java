/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.configuration;

public class DebeziumProperties {

    // Debezium property prefixes
    public static final String PROP_PREFIX = "debezium.";
    public static final String PROP_SOURCE_PREFIX = PROP_PREFIX + "source.";
    public static final String PROP_SINK_PREFIX = PROP_PREFIX + "sink.";
    public static final String PROP_FORMAT_PREFIX = PROP_PREFIX + "format.";
    public static final String PROP_PREDICATES_PREFIX = PROP_PREFIX + "predicates.";
    public static final String PROP_TRANSFORMS_PREFIX = PROP_PREFIX + "transforms.";

    // Format sub-prefixes (with trailing dot for property enumeration)
    public static final String PROP_KEY_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "key.";
    public static final String PROP_VALUE_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "value.";
    public static final String PROP_HEADER_FORMAT_PREFIX = PROP_FORMAT_PREFIX + "header.";

    // Format property names (without trailing dot, for direct lookups)
    public static final String PROP_KEY_FORMAT = PROP_FORMAT_PREFIX + "key";
    public static final String PROP_VALUE_FORMAT = PROP_FORMAT_PREFIX + "value";
    public static final String PROP_HEADER_FORMAT = PROP_FORMAT_PREFIX + "header";

    // Sink properties
    public static final String PROP_SINK_TYPE = PROP_SINK_PREFIX + "type";

    // Transform and predicate properties
    public static final String PROP_PREDICATES = PROP_PREFIX + "predicates";
    public static final String PROP_TRANSFORMS = PROP_PREFIX + "transforms";

    // Offset storage
    public static final String PROP_OFFSET_STORAGE_PREFIX = "offset.storage.";

    // Quarkus mapping prefixes
    public static final String QUARKUS_DEBEZIUM_PREFIX = "quarkus.debezium.";
    public static final String QUARKUS_DATASOURCE_PREFIX = "quarkus.datasource.";
    public static final String DEBEZIUM_DATASOURCE_PREFIX = PROP_SOURCE_PREFIX + "datasource.";

    // Quarkus converter prefixes
    public static final String QUARKUS_KEY_CONVERTER_PREFIX = QUARKUS_DEBEZIUM_PREFIX + "key.converter.";
    public static final String QUARKUS_VALUE_CONVERTER_PREFIX = QUARKUS_DEBEZIUM_PREFIX + "value.converter.";
    public static final String QUARKUS_HEADER_CONVERTER_PREFIX = QUARKUS_DEBEZIUM_PREFIX + "header.converter.";

    // Schema Registry / Apicurio prefixes
    public static final String PROP_SCHEMA_REGISTRY_URL = PROP_FORMAT_PREFIX + "schema.registry.url";
    public static final String PROP_FORMAT_KEY_APICURIO_PREFIX = PROP_KEY_FORMAT + ".apicurio.registry.";
    public static final String PROP_FORMAT_VALUE_APICURIO_PREFIX = PROP_VALUE_FORMAT + ".apicurio.registry.";
    public static final String PROP_FORMAT_HEADER_APICURIO_PREFIX = PROP_HEADER_FORMAT + ".apicurio.registry.";
    public static final String PROP_FORMAT_APICURIO_PREFIX = PROP_FORMAT_PREFIX + "apicurio.registry.";

    // Empty value sentinel used by DebeziumServerConfigSourceFactory and EmptyStringConverter
    public static final String EMPTY_VALUE_SENTINEL = "__DBZ_EMPTY__";
}
