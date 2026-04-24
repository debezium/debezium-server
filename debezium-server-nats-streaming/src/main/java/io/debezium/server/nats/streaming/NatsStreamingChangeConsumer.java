/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.nats.streaming;

import java.util.List;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.api.DebeziumServerConsumer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.config.Field;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;
import io.debezium.server.api.DebeziumServerSink;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.streaming.NatsStreaming;
import io.nats.streaming.StreamingConnection;

/**
 * Implementation of the consumer that delivers the messages into NATS Streaming subject.
 *
 * @author Thiago Avancini
 */
@Named("nats-streaming")
@Dependent
public class NatsStreamingChangeConsumer extends BaseChangeConsumer
        implements DebeziumServerConsumer<CapturingEvents<BatchEvent>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(NatsStreamingChangeConsumer.class);

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    private static final String PROP_PREFIX = "debezium.sink.nats-streaming.";

    private NatsStreamingChangeConsumerConfig config;

    private Connection nc;
    private StreamingConnection sc;

    @Inject
    @CustomConsumerBuilder
    Instance<StreamingConnection> customStreamingConnection;

    @PostConstruct
    void connect() {
        if (customStreamingConnection.isResolvable()) {
            sc = customStreamingConnection.get();
            LOGGER.info("Obtained custom configured StreamingConnection '{}'", sc);
            return;
        }

        final Config mpConfig = ConfigProvider.getConfig();

        // Load configuration
        io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));
        this.config = new NatsStreamingChangeConsumerConfig(configuration);

        try {
            // Setup NATS connection
            io.nats.client.Options natsOptions = new io.nats.client.Options.Builder()
                    .server(config.getUrl())
                    .noReconnect()
                    .build();
            nc = Nats.connect(natsOptions);

            // Setup NATS Streaming connection
            io.nats.streaming.Options stanOptions = new io.nats.streaming.Options.Builder()
                    .natsConn(nc)
                    .build();
            sc = NatsStreaming.connect(config.getClusterId(), config.getClientId(), stanOptions);
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }

        LOGGER.info("Using default StreamingConnection '{}'", sc);
    }

    @PreDestroy
    @Override
    public void close() {
        try {
            if (sc != null) {
                sc.close();
                LOGGER.info("NATS Streaming connection closed.");
            }

            if (nc != null) {
                nc.close();
                LOGGER.info("NATS connection closed.");
            }
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    @Override
    public void handle(CapturingEvents<BatchEvent> events) {

        for (BatchEvent record : events.records()) {
            if (record.value() != null) {
                String subject = streamNameMapper.map(events.destination());
                byte[] recordBytes = getBytes(record.value());
                LOGGER.trace("Received event @ {} = '{}'", subject, record.value());

                try {
                    sc.publish(subject, recordBytes);
                }
                catch (Exception e) {
                    throw new DebeziumException(e);
                }
            }
            record.commit();
        }
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                NatsStreamingChangeConsumerConfig.URL,
                NatsStreamingChangeConsumerConfig.CLUSTER_ID,
                NatsStreamingChangeConsumerConfig.CLIENT_ID);
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }
}
