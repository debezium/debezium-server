/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.nats.streaming;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link NatsStreamingChangeConsumer}.
 */
public class NatsStreamingChangeConsumerConfig {

    public static final Field URL = Field.create("url")
            .withDisplayName("URL")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("NATS server URL.");

    public static final Field CLUSTER_ID = Field.create("cluster.id")
            .withDisplayName("Cluster ID")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("NATS Streaming cluster ID.");

    public static final Field CLIENT_ID = Field.create("client.id")
            .withDisplayName("Client ID")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("NATS Streaming client ID.");

    // Instance fields
    private String url;
    private String clusterId;
    private String clientId;

    public NatsStreamingChangeConsumerConfig(Configuration config) {
        init(config);
    }

    protected void init(Configuration config) {
        url = config.getString(URL);
        clusterId = config.getString(CLUSTER_ID);
        clientId = config.getString(CLIENT_ID);
    }

    public String getUrl() {
        return url;
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getClientId() {
        return clientId;
    }
}
