package io.debezium.server.configuration;

public class DebeziumProperties {
    public static final String PROP_PREFIX = "debezium.";
    public static final String PROP_FORMAT_PREFIX = PROP_PREFIX + "format.";
    public static final String PROP_KEY_FORMAT = PROP_FORMAT_PREFIX + "key";
    public static final String PROP_VALUE_FORMAT = PROP_FORMAT_PREFIX + "value";
    public static final String PROP_HEADER_FORMAT = PROP_FORMAT_PREFIX + "header";
    public static final String PROP_SOURCE_PREFIX = PROP_PREFIX + "source.";

    public static final String PROP_SINK_PREFIX = PROP_PREFIX + "sink.";
    public static final String PROP_SINK_TYPE = PROP_SINK_PREFIX + "type";
}
