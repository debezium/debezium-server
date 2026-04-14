/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.infinispan;

import org.apache.kafka.common.config.ConfigDef;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link InfinispanSinkConsumer}.
 */
public class InfinispanSinkConsumerConfig {

    public static final Field SERVER_HOST = Field.create("server.host")
            .withDisplayName("Server Host")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Infinispan server host.");

    public static final Field SERVER_PORT = Field.create("server.port")
            .withDisplayName("Server Port")
            .withType(ConfigDef.Type.INT)
            .withDefault(ConfigurationProperties.DEFAULT_HOTROD_PORT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Infinispan server port.");

    public static final Field CACHE = Field.create("cache")
            .withDisplayName("Cache Name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Infinispan cache name.");

    public static final Field USER = Field.create("user")
            .withDisplayName("User")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Infinispan user.");

    public static final Field PASSWORD = Field.create("password")
            .withDisplayName("Password")
            .withType(ConfigDef.Type.PASSWORD)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Infinispan password.");

    // Instance fields
    private String serverHost;
    private Integer serverPort;
    private String cacheName;
    private String user;
    private String password;

    public InfinispanSinkConsumerConfig(Configuration config) {
        init(config);
    }

    protected void init(Configuration config) {
        serverHost = config.getString(SERVER_HOST);
        serverPort = config.getInteger(SERVER_PORT);
        cacheName = config.getString(CACHE);
        user = config.getString(USER);
        password = config.getString(PASSWORD);
    }

    public String getServerHost() {
        return serverHost;
    }

    public Integer getServerPort() {
        return serverPort;
    }

    public String getCacheName() {
        return cacheName;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }
}
