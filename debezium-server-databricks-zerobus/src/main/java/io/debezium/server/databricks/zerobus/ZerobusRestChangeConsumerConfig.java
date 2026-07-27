/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link ZerobusRestChangeConsumer} (the REST route).
 * Properties are prefixed with {@code debezium.sink.zerobusrest.} in {@code application.properties}.
 */
public class ZerobusRestChangeConsumerConfig {

    public static final Field URI = Field.create("uri")
            .withDisplayName("Zerobus REST base URI")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("Zerobus REST base URI, e.g. https://<workspace-id>.zerobus.<region>.cloud.databricks.com");

    public static final Field WORKSPACE_URL = Field.create("workspace.url")
            .withDisplayName("Databricks workspace URL")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("Databricks workspace URL used for OAuth and the unity-catalog-endpoint header.");

    public static final Field WORKSPACE_ID = Field.create("workspace.id")
            .withDisplayName("Databricks workspace id")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("Numeric workspace id, used to build the Zerobus token audience.");

    public static final Field CLIENT_ID = Field.create("client.id")
            .withDisplayName("Service principal client id")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("OAuth client id (service principal application id).");

    public static final Field CLIENT_SECRET = Field.create("client.secret")
            .withDisplayName("Service principal client secret")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("OAuth client secret for the service principal.");

    public static final Field TABLE = Field.create("table")
            .withDisplayName("Default target table")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Fully qualified default target table (catalog.schema.table). Used when the mapped "
                    + "topic name is not already a fully qualified table name.");

    private final String uri;
    private final String workspaceUrl;
    private final String workspaceId;
    private final String clientId;
    private final String clientSecret;
    private final String table;

    public ZerobusRestChangeConsumerConfig(Configuration config) {
        this.uri = config.getString(URI);
        this.workspaceUrl = config.getString(WORKSPACE_URL);
        this.workspaceId = config.getString(WORKSPACE_ID);
        this.clientId = config.getString(CLIENT_ID);
        this.clientSecret = config.getString(CLIENT_SECRET);
        this.table = config.getString(TABLE);
    }

    public String getUri() {
        return uri;
    }

    public String getWorkspaceUrl() {
        return workspaceUrl;
    }

    public String getWorkspaceId() {
        return workspaceId;
    }

    public String getClientId() {
        return clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public String getTable() {
        return table;
    }
}
