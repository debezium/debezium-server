/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.milvus;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link MilvusChangeConsumer}.
 */
public class MilvusChangeConsumerConfig {

    public static final Field URI = Field.create("uri")
            .withDisplayName("URI")
            .withType(ConfigDef.Type.STRING)
            .withDefault("http://localhost:19530")
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Milvus server URI.");

    public static final Field DATABASE = Field.create("database")
            .withDisplayName("Database")
            .withType(ConfigDef.Type.STRING)
            .withDefault("default")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Milvus database name.");

    public static final Field UNWIND_JSON = Field.create("unwind.json")
            .withDisplayName("Unwind JSON")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Unwind JSON fields.");

    // Instance fields
    private String uri;
    private String databaseName;
    private boolean unwindJson;

    public MilvusChangeConsumerConfig(Configuration config) {
        init(config);
    }

    protected void init(Configuration config) {
        uri = config.getString(URI);
        databaseName = config.getString(DATABASE);
        unwindJson = config.getBoolean(UNWIND_JSON, false);
    }

    public String getUri() {
        return uri;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public boolean isUnwindJson() {
        return unwindJson;
    }
}
