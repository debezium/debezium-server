/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.qdrant;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link QdrantChangeConsumer}.
 */
public class QdrantChangeConsumerConfig {

    public static final Field HOST = Field.create("host")
            .withDisplayName("Host")
            .withType(ConfigDef.Type.STRING)
            .withDefault("localhost")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Qdrant server host.");

    public static final Field PORT = Field.create("port")
            .withDisplayName("Port")
            .withType(ConfigDef.Type.INT)
            .withDefault(6333)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Qdrant server port.");

    public static final Field API_KEY = Field.create("api.key")
            .withDisplayName("API Key")
            .withType(ConfigDef.Type.PASSWORD)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Qdrant API key for authentication.");

    public static final Field VECTOR_FIELD_NAMES = Field.create("vector.field.names")
            .withDisplayName("Vector Field Names")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Comma-separated list of vector field names.");

    // Instance fields
    private String host;
    private int port;
    private String apiKey;
    private String vectorFieldNames;

    public QdrantChangeConsumerConfig(Configuration config) {
        init(config);
    }

    protected void init(Configuration config) {
        host = config.getString(HOST);
        port = config.getInteger(PORT, 6333);
        apiKey = config.getString(API_KEY);
        vectorFieldNames = config.getString(VECTOR_FIELD_NAMES);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getApiKey() {
        return apiKey;
    }

    public String getVectorFieldNames() {
        return vectorFieldNames;
    }
}
