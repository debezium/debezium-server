/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rocketmq;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByHash;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.config.Field;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;
import io.debezium.server.DebeziumServerSink;

/**
 * rocketmq change consumer
 */
@Named("rocketmq")
@Dependent
public class RocketMqChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocketMqChangeConsumer.class);

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    private static final String PROP_PREFIX = "debezium.sink.rocketmq.";

    private RocketMqChangeConsumerConfig config;

    @Inject
    @CustomConsumerBuilder
    Instance<DefaultMQProducer> customRocketMqProducer;
    private DefaultMQProducer mqProducer;

    @PostConstruct
    void connect() {
        if (customRocketMqProducer.isResolvable()) {
            mqProducer = customRocketMqProducer.get();
            startProducer();
            LOGGER.info("Obtained custom configured RocketMqProducer '{}'", mqProducer);
            return;
        }

        final Config mpConfig = ConfigProvider.getConfig();

        // Load configuration
        io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));
        this.config = new RocketMqChangeConsumerConfig(configuration);

        // init rocketmq producer
        RPCHook rpcHook = null;
        if (config.isAclEnabled()) {
            if (config.getAccessKey() == null || config.getAccessKey().isEmpty()
                    || config.getSecretKey() == null || config.getSecretKey().isEmpty()) {
                throw new DebeziumException("When acl.enabled is true, access key and secret key cannot be empty");
            }
            rpcHook = new AclClientRPCHook(
                    new SessionCredentials(config.getAccessKey(), config.getSecretKey()));
        }
        this.mqProducer = new DefaultMQProducer(rpcHook);
        this.mqProducer.setNamesrvAddr(config.getNameSrvAddr());
        this.mqProducer.setInstanceName(createUniqInstance(config.getNameSrvAddr()));
        this.mqProducer.setProducerGroup(config.getProducerGroup());

        if (config.getSendMsgTimeout() != null) {
            this.mqProducer.setSendMsgTimeout(config.getSendMsgTimeout());
        }

        if (config.getMaxMessageSize() != null) {
            this.mqProducer.setMaxMessageSize(config.getMaxMessageSize());
        }

        this.mqProducer.setLanguage(LanguageCode.JAVA);
        startProducer();
    }

    private void startProducer() {
        try {
            this.mqProducer.start();
            LOGGER.info("Consumer started...");
        }
        catch (MQClientException e) {
            throw new DebeziumException(e);
        }
    }

    private String createUniqInstance(String prefix) {
        return prefix.concat("-").concat(UUID.randomUUID().toString());
    }

    @PreDestroy
    @Override
    public void close() {
        // Closed rocketmq producer
        LOGGER.info("Consumer destroy...");
        if (mqProducer != null) {
            mqProducer.shutdown();
        }
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                RocketMqChangeConsumerConfig.PRODUCER_ACL_ENABLED,
                RocketMqChangeConsumerConfig.PRODUCER_ACCESS_KEY,
                RocketMqChangeConsumerConfig.PRODUCER_SECRET_KEY,
                RocketMqChangeConsumerConfig.PRODUCER_NAME_SRV_ADDR,
                RocketMqChangeConsumerConfig.PRODUCER_GROUP,
                RocketMqChangeConsumerConfig.PRODUCER_MAX_MESSAGE_SIZE,
                RocketMqChangeConsumerConfig.PRODUCER_SEND_MSG_TIMEOUT);
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(records.size());
        for (ChangeEvent<Object, Object> record : records) {
            try {
                final String topicName = streamNameMapper.map(record.destination());
                String key = getString(record.key());

                Message message = new Message(topicName, null, key, getBytes(record.value()));

                Map<String, String> headers = convertHeaders(record);
                for (Map.Entry<String, String> entry : headers.entrySet()) {
                    message.putUserProperty(entry.getKey(), entry.getValue());
                }

                mqProducer.send(message, new SelectMessageQueueByHash(), key, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        LOGGER.debug("Sent message with offset: {}", sendResult.getQueueOffset());
                        latch.countDown();
                    }

                    @Override
                    public void onException(Throwable throwable) {
                        LOGGER.error("Failed to send record to {}:", record.destination(), throwable);
                        throw new DebeziumException(throwable);
                    }
                });
            }
            catch (Exception e) {
                throw new DebeziumException(e);
            }
        }

        // Messages have set default send timeout, so this will not block forever.
        latch.await();

        for (ChangeEvent<Object, Object> record : records) {
            committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }

}
