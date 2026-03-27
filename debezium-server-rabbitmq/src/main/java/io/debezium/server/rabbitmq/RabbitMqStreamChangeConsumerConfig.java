/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rabbitmq;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link RabbitMqStreamChangeConsumer}.
 */
public class RabbitMqStreamChangeConsumerConfig {

    public static final String STATIC_ROUTING_KEY_SOURCE = "static";

    public static final Field EXCHANGE = Field.create("exchange")
            .withDisplayName("Exchange")
            .withType(ConfigDef.Type.STRING)
            .withDefault("")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("RabbitMQ exchange name.");

    public static final Field ROUTING_KEY = Field.create("routingKey")
            .withDisplayName("Routing Key")
            .withType(ConfigDef.Type.STRING)
            .withDefault("")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("RabbitMQ routing key.");

    public static final Field AUTO_CREATE_ROUTING_KEY = Field.create("autoCreateRoutingKey")
            .withDisplayName("Auto Create Routing Key")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Automatically create routing key if it doesn't exist.");

    public static final Field ROUTING_KEY_DURABLE = Field.create("routingKeyDurable")
            .withDisplayName("Routing Key Durable")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(true)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Make routing key durable.");

    public static final Field ROUTING_KEY_SOURCE = Field.create("routingKey.source")
            .withDisplayName("Routing Key Source")
            .withType(ConfigDef.Type.STRING)
            .withDefault(STATIC_ROUTING_KEY_SOURCE)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Source for routing key value.");

    public static final Field ROUTING_KEY_FROM_TOPIC_NAME = Field.create("routingKeyFromTopicName")
            .withDisplayName("Routing Key From Topic Name")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Use topic name as routing key.");

    public static final Field DELIVERY_MODE = Field.create("deliveryMode")
            .withDisplayName("Delivery Mode")
            .withType(ConfigDef.Type.INT)
            .withDefault(2)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Message delivery mode (1=non-persistent, 2=persistent).");

    public static final Field ACK_TIMEOUT = Field.create("ackTimeout")
            .withDisplayName("Ack Timeout (ms)")
            .withType(ConfigDef.Type.INT)
            .withDefault(30000)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Acknowledgment timeout in milliseconds.");

    public static final Field NULL_VALUE = Field.create("null.value")
            .withDisplayName("Null Value")
            .withType(ConfigDef.Type.STRING)
            .withDefault("default")
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Value to use when record value is null.");

    // Instance fields
    private String exchange;
    private String routingKey;
    private boolean autoCreateRoutingKey;
    private boolean routingKeyDurable;
    private String routingKeySource;
    private boolean routingKeyFromTopicName;
    private int deliveryMode;
    private int ackTimeout;
    private String nullValue;

    public RabbitMqStreamChangeConsumerConfig(Configuration config) {
        init(config);
    }

    protected void init(Configuration config) {
        exchange = config.getString(EXCHANGE);
        routingKey = config.getString(ROUTING_KEY);
        autoCreateRoutingKey = config.getBoolean(AUTO_CREATE_ROUTING_KEY, false);
        routingKeyDurable = config.getBoolean(ROUTING_KEY_DURABLE, true);
        routingKeySource = config.getString(ROUTING_KEY_SOURCE);
        routingKeyFromTopicName = config.getBoolean(ROUTING_KEY_FROM_TOPIC_NAME, false);
        deliveryMode = config.getInteger(DELIVERY_MODE, 2);
        ackTimeout = config.getInteger(ACK_TIMEOUT, 30000);
        nullValue = config.getString(NULL_VALUE);
    }

    public String getExchange() {
        return exchange;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public boolean isAutoCreateRoutingKey() {
        return autoCreateRoutingKey;
    }

    public boolean isRoutingKeyDurable() {
        return routingKeyDurable;
    }

    public String getRoutingKeySource() {
        return routingKeySource;
    }

    public boolean isRoutingKeyFromTopicName() {
        return routingKeyFromTopicName;
    }

    public int getDeliveryMode() {
        return deliveryMode;
    }

    public int getAckTimeout() {
        return ackTimeout;
    }

    public String getNullValue() {
        return nullValue;
    }
}
