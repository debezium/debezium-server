/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rabbitmq;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.StreamNameMapper;

class RabbitMqStreamChangeConsumerTest {

    private static final int DELIVERY_MODE = 2;
    private static final int ACK_TIMEOUT = 1000;

    private Channel channelMock;
    private StreamNameMapper streamNameMapperMock;
    private ChangeEvent<Object, Object> eventMock;
    private RecordCommitter<ChangeEvent<Object, Object>> committerMock;

    private RabbitMqStreamChangeConsumer rabbitMqStreamChangeConsumer;

    public static Stream<Arguments> testHandleBatch_StaticRoutingKeySourceParameters() {
        return Stream.of(
                Arguments.of("static-routing-key", "static-routing-key"),
                Arguments.of(null, ""));
    }

    public static Stream<Arguments> testHandleBatch_KeyRoutingKeySourceParameters() {
        return Stream.of(
                Arguments.of("test-routing-key", "test-routing-key"),
                Arguments.of(null, ""));
    }

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        eventMock = mock(ChangeEvent.class);
        channelMock = mock(Channel.class);
        committerMock = mock(RecordCommitter.class);
        streamNameMapperMock = mock(StreamNameMapper.class);

        rabbitMqStreamChangeConsumer = new RabbitMqStreamChangeConsumer();
        rabbitMqStreamChangeConsumer.channel = channelMock;
        rabbitMqStreamChangeConsumer.setStreamNameMapper(streamNameMapperMock);
        rabbitMqStreamChangeConsumer.deliveryMode = DELIVERY_MODE;
        rabbitMqStreamChangeConsumer.ackTimeout = ACK_TIMEOUT;
    }

    @Test
    void testHandleBatch_TopicRoutingKeySource() throws InterruptedException, IOException, TimeoutException {
        // given
        String topicName = "test-topic";
        String payload = "test content";
        List<ChangeEvent<Object, Object>> records = List.of(eventMock);

        when(eventMock.destination()).thenReturn(topicName);
        when(eventMock.value()).thenReturn(payload);
        when(eventMock.headers()).thenReturn(List.of());
        when(streamNameMapperMock.map(topicName)).thenReturn(topicName);

        rabbitMqStreamChangeConsumer.exchange = Optional.of(topicName);
        rabbitMqStreamChangeConsumer.routingKeySource = "topic";
        rabbitMqStreamChangeConsumer.autoCreateRoutingKey = true;
        rabbitMqStreamChangeConsumer.routingKeyDurable = true;
        rabbitMqStreamChangeConsumer.routingKey = Optional.of("ignored");

        // when
        rabbitMqStreamChangeConsumer.handleBatch(records, committerMock);

        // then
        verify(channelMock).queueDeclare(topicName, true, false, false, null);

        final AMQP.BasicProperties expectedProperties = new AMQP.BasicProperties.Builder()
                .deliveryMode(DELIVERY_MODE)
                .headers(Map.of())
                .build();

        verify(channelMock).basicPublish(topicName, topicName, expectedProperties, payload.getBytes());
        verify(channelMock).waitForConfirmsOrDie(ACK_TIMEOUT);
    }

    @ParameterizedTest
    @MethodSource("testHandleBatch_StaticRoutingKeySourceParameters")
    void testHandleBatch_StaticRoutingKeySource(String staticRoutingKey, String expectedRoutingKey) throws InterruptedException, IOException, TimeoutException {
        // given
        String topicName = "test-topic";
        String payload = "test content";
        List<ChangeEvent<Object, Object>> records = List.of(eventMock);

        when(eventMock.destination()).thenReturn(topicName);
        when(eventMock.value()).thenReturn(payload);
        when(eventMock.headers()).thenReturn(List.of());
        when(streamNameMapperMock.map(topicName)).thenReturn(topicName);

        rabbitMqStreamChangeConsumer.exchange = Optional.of(topicName);
        rabbitMqStreamChangeConsumer.routingKeySource = "static";
        rabbitMqStreamChangeConsumer.routingKey = Optional.ofNullable(staticRoutingKey);

        // when
        rabbitMqStreamChangeConsumer.handleBatch(records, committerMock);

        // then
        verify(channelMock, never()).queueDeclare(any(), anyBoolean(), anyBoolean(), anyBoolean(), any());

        final AMQP.BasicProperties expectedProperties = new AMQP.BasicProperties.Builder()
                .deliveryMode(DELIVERY_MODE)
                .headers(Map.of())
                .build();

        verify(channelMock).basicPublish(topicName, expectedRoutingKey, expectedProperties, payload.getBytes());
        verify(channelMock).waitForConfirmsOrDie(ACK_TIMEOUT);
    }

    @ParameterizedTest
    @MethodSource("testHandleBatch_KeyRoutingKeySourceParameters")
    void testHandleBatch_KeyRoutingKeySource(String routingKey, String expectedRoutingKey) throws InterruptedException, IOException, TimeoutException {
        // given
        String topicName = "test-topic";
        String payload = "test content";
        List<ChangeEvent<Object, Object>> records = List.of(eventMock);

        when(eventMock.destination()).thenReturn(topicName);
        when(eventMock.value()).thenReturn(payload);
        when(eventMock.key()).thenReturn(routingKey);
        when(eventMock.headers()).thenReturn(List.of());
        when(streamNameMapperMock.map(topicName)).thenReturn(topicName);
        when(streamNameMapperMock.map(routingKey)).thenReturn(routingKey);

        rabbitMqStreamChangeConsumer.exchange = Optional.of(topicName);
        rabbitMqStreamChangeConsumer.routingKeySource = "key";
        rabbitMqStreamChangeConsumer.routingKey = Optional.of("ignored");

        // when
        rabbitMqStreamChangeConsumer.handleBatch(records, committerMock);

        // then
        verify(channelMock, never()).queueDeclare(any(), anyBoolean(), anyBoolean(), anyBoolean(), any());

        final AMQP.BasicProperties expectedProperties = new AMQP.BasicProperties.Builder()
                .deliveryMode(DELIVERY_MODE)
                .headers(Map.of())
                .build();

        verify(channelMock).basicPublish(topicName, expectedRoutingKey, expectedProperties, payload.getBytes());
        verify(channelMock).waitForConfirmsOrDie(ACK_TIMEOUT);
    }
}
