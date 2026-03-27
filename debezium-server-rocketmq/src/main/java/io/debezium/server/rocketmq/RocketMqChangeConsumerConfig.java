/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rocketmq;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link RocketMqChangeConsumer}.
 */
public class RocketMqChangeConsumerConfig {

    public static final Field PRODUCER_ACL_ENABLED = Field.create("producer.acl.enabled")
            .withDisplayName("ACL Enabled")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Enable ACL (Access Control List) authentication.");

    public static final Field PRODUCER_ACCESS_KEY = Field.create("producer.access.key")
            .withDisplayName("Access Key")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Access key for ACL authentication (required when ACL is enabled).");

    public static final Field PRODUCER_SECRET_KEY = Field.create("producer.secret.key")
            .withDisplayName("Secret Key")
            .withType(ConfigDef.Type.PASSWORD)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Secret key for ACL authentication (required when ACL is enabled).");

    public static final Field PRODUCER_NAME_SRV_ADDR = Field.create("producer.name.srv.addr")
            .withDisplayName("Name Server Address")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("RocketMQ name server address.");

    public static final Field PRODUCER_GROUP = Field.create("producer.group")
            .withDisplayName("Producer Group")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("RocketMQ producer group name.");

    public static final Field PRODUCER_MAX_MESSAGE_SIZE = Field.create("producer.max.message.size")
            .withDisplayName("Max Message Size")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum message size in bytes.");

    public static final Field PRODUCER_SEND_MSG_TIMEOUT = Field.create("producer.send.msg.timeout")
            .withDisplayName("Send Message Timeout (ms)")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Timeout in milliseconds for sending messages.");

    // Instance fields
    private boolean aclEnabled;
    private String accessKey;
    private String secretKey;
    private String nameSrvAddr;
    private String producerGroup;
    private Integer maxMessageSize;
    private Integer sendMsgTimeout;

    public RocketMqChangeConsumerConfig(Configuration config) {
        init(config);
    }

    protected void init(Configuration config) {
        aclEnabled = config.getBoolean(PRODUCER_ACL_ENABLED, false);
        accessKey = config.getString(PRODUCER_ACCESS_KEY);
        secretKey = config.getString(PRODUCER_SECRET_KEY);
        nameSrvAddr = config.getString(PRODUCER_NAME_SRV_ADDR);
        producerGroup = config.getString(PRODUCER_GROUP);
        // Optional fields - handle null case
        String maxMessageSizeStr = config.getString(PRODUCER_MAX_MESSAGE_SIZE);
        maxMessageSize = (maxMessageSizeStr != null) ? Integer.valueOf(maxMessageSizeStr) : null;
        String sendMsgTimeoutStr = config.getString(PRODUCER_SEND_MSG_TIMEOUT);
        sendMsgTimeout = (sendMsgTimeoutStr != null) ? Integer.valueOf(sendMsgTimeoutStr) : null;
    }

    public boolean isAclEnabled() {
        return aclEnabled;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getNameSrvAddr() {
        return nameSrvAddr;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public Integer getMaxMessageSize() {
        return maxMessageSize;
    }

    public Integer getSendMsgTimeout() {
        return sendMsgTimeout;
    }
}
