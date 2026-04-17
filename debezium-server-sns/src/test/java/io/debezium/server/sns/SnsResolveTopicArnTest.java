/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.sns;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

/**
 * Unit tests for {@link SnsChangeConsumer#resolveTopicArn(String)} routing logic.
 */
class SnsResolveTopicArnTest {

    @Test
    void fullArnIsReturnedDirectly() {
        SnsChangeConsumer consumer = consumerWithConfig(Map.of("region", "us-east-1"));
        assertEquals("arn:aws:sns:us-east-1:123456789:my-topic",
                consumer.resolveTopicArn("arn:aws:sns:us-east-1:123456789:my-topic"));
    }

    @Test
    void prefixIsComposedWithDestination() {
        SnsChangeConsumer consumer = consumerWithConfig(Map.of(
                "region", "us-east-1",
                "topic.arn.prefix", "arn:aws:sns:us-east-1:000000000000:"));

        assertEquals("arn:aws:sns:us-east-1:000000000000:User", consumer.resolveTopicArn("User"));
        assertEquals("arn:aws:sns:us-east-1:000000000000:Order", consumer.resolveTopicArn("Order"));
    }

    @Test
    void defaultTopicArnIsUsedWhenNoPrefixAndNotFullArn() {
        SnsChangeConsumer consumer = consumerWithConfig(Map.of(
                "region", "us-east-1",
                "topic.arn", "arn:aws:sns:us-east-1:000000000000:default-topic"));

        assertEquals("arn:aws:sns:us-east-1:000000000000:default-topic",
                consumer.resolveTopicArn("some-destination"));
    }

    @Test
    void prefixTakesPrecedenceOverDefaultTopicArn() {
        SnsChangeConsumer consumer = consumerWithConfig(Map.of(
                "region", "us-east-1",
                "topic.arn.prefix", "arn:aws:sns:us-east-1:000000000000:",
                "topic.arn", "arn:aws:sns:us-east-1:000000000000:default-topic"));

        assertEquals("arn:aws:sns:us-east-1:000000000000:Order", consumer.resolveTopicArn("Order"));
    }

    @Test
    void fullArnTakesPrecedenceOverPrefix() {
        SnsChangeConsumer consumer = consumerWithConfig(Map.of(
                "region", "us-east-1",
                "topic.arn.prefix", "arn:aws:sns:us-east-1:000000000000:"));

        assertEquals("arn:aws:sns:eu-west-1:999999999999:other-topic",
                consumer.resolveTopicArn("arn:aws:sns:eu-west-1:999999999999:other-topic"));
    }

    @Test
    void destinationIsReturnedAsIsWhenNoPrefixAndNoDefault() {
        SnsChangeConsumer consumer = consumerWithConfig(Map.of("region", "us-east-1"));
        assertEquals("my-destination", consumer.resolveTopicArn("my-destination"));
    }

    private static SnsChangeConsumer consumerWithConfig(Map<String, String> props) {
        SnsChangeConsumer consumer = new SnsChangeConsumer();
        consumer.config = new SnsChangeConsumerConfig(Configuration.from(props));
        return consumer;
    }
}
