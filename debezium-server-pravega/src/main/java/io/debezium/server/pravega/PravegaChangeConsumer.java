/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pravega;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.api.DebeziumServerConsumer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.Module;
import io.debezium.config.Field;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.ChangeConsumer;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.api.DebeziumServerSink;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.ByteArraySerializer;

@Named("pravega")
@Dependent
public class PravegaChangeConsumer extends BaseChangeConsumer implements DebeziumServerConsumer<CapturingEvents<BatchEvent>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(PravegaChangeConsumer.class);

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    private static final String PROP_PREFIX = "debezium.sink.pravega.";

    private PravegaChangeConsumerConfig config;

    private ClientConfig clientConfig;
    private EventStreamClientFactory factory;
    private EventWriterConfig writerConfig;

    @PostConstruct
    void constructor() {
        final Config mpConfig = ConfigProvider.getConfig();

        // Load configuration
        io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));
        this.config = new PravegaChangeConsumerConfig(configuration);

        clientConfig = ClientConfig.builder()
                .controllerURI(config.getControllerUri())
                .build();
        LOGGER.debug("Creating client factory for scope {} with controller {}", config.getScope(), config.getControllerUri());
        factory = EventStreamClientFactory.withScope(config.getScope(), clientConfig);
        writerConfig = EventWriterConfig.builder().build();
    }

    @PreDestroy
    @Override
    public void close() {
        LOGGER.debug("Closing client factory");
        factory.close();
    }

    @Override
    public void handle(CapturingEvents<BatchEvent> events) {
        try (PravegaSink impl = (config.isTransaction()) ? new PravegaTxnSinkImpl() : new PravegaSinkImpl()) {
            impl.handle(events);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    class PravegaSinkImpl implements PravegaSink {
        private final Map<String, EventStreamWriter<byte[]>> writers = new HashMap<>();

        @Override
        public void handle(CapturingEvents<BatchEvent> events) {
            for (BatchEvent changeEvent : events.records()) {
                String streamName = streamNameMapper.map(events.destination());
                final EventStreamWriter<byte[]> writer = writers.computeIfAbsent(streamName, (stream) -> createWriter(stream));
                if (changeEvent.key() != null) {
                    writer.writeEvent(getString(changeEvent.key()), getBytes(changeEvent.value()));
                }
                else {
                    writer.writeEvent(getBytes(changeEvent.value()));
                }
                changeEvent.commit();
            }
        }

        private EventStreamWriter<byte[]> createWriter(String stream) {
            LOGGER.debug("Creating writer for stream {}", stream);
            return factory.createEventWriter(stream, new ByteArraySerializer(), writerConfig);
        }

        @Override
        public void close() throws Exception {
            LOGGER.debug("Closing {} writer(s)", writers.size());
            writers.values().forEach(EventStreamWriter::close);
        }
    }

    class PravegaTxnSinkImpl implements PravegaSink {
        private final Map<String, TransactionalEventStreamWriter<byte[]>> writers = new HashMap<>();
        private final Map<String, Transaction<byte[]>> txns = new HashMap<>();

        @Override
        public void handle(CapturingEvents<BatchEvent> events) {
            for (BatchEvent changeEvent : events.records()) {
                String streamName = streamNameMapper.map(events.destination());
                final Transaction<byte[]> txn = txns.computeIfAbsent(streamName, (stream) -> createTxn(stream));
                try {
                    if (changeEvent.key() != null) {
                        txn.writeEvent(getString(changeEvent.key()), getBytes(changeEvent.value()));
                    }
                    else {
                        txn.writeEvent(getBytes(changeEvent.value()));
                    }
                }
                catch (TxnFailedException e) {
                    throw new RuntimeException(e);
                }
                changeEvent.commit();
            }
            txns.values().forEach(t -> {
                try {
                    t.commit();
                }
                catch (TxnFailedException e) {
                    throw new RuntimeException(e);
                }
            });
            txns.clear();
        }

        private Transaction<byte[]> createTxn(String stream) {
            final TransactionalEventStreamWriter<byte[]> writer = writers.computeIfAbsent(stream, (s) -> createWriter(s));
            LOGGER.debug("Creating transaction for stream {}", stream);
            return writer.beginTxn();
        }

        private TransactionalEventStreamWriter<byte[]> createWriter(String stream) {
            LOGGER.debug("Creating writer for stream {}", stream);
            return factory.createTransactionalEventWriter(stream, new ByteArraySerializer(), writerConfig);
        }

        @Override
        public void close() throws Exception {
            LOGGER.debug("Closing {} writer(s)", writers.size());
            writers.values().forEach(TransactionalEventStreamWriter::close);
        }
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                PravegaChangeConsumerConfig.CONTROLLER_URI,
                PravegaChangeConsumerConfig.SCOPE,
                PravegaChangeConsumerConfig.TRANSACTION);
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }

}
