/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ydb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.api.DebeziumServerSink;
import io.debezium.util.Strings;

import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.description.Codec;
import tech.ydb.topic.description.Consumer;
import tech.ydb.topic.description.MetadataItem;
import tech.ydb.topic.description.SupportedCodecs;
import tech.ydb.topic.description.TopicDescription;
import tech.ydb.topic.settings.CreateTopicSettings;
import tech.ydb.topic.settings.PartitioningSettings;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.AsyncWriter;
import tech.ydb.topic.write.Message;
import tech.ydb.topic.write.WriteAck;

/**
 * Debezium Server sink that writes change events to YDB Topics using {@link AsyncWriter}.
 * <p>
 * Delivery is at-least-once: offsets are committed only after all {@code send()} acknowledgements
 * in the batch succeed. Tombstone records (null value) are skipped without a write but still
 * marked processed.
 * <p>
 * Each process must use a distinct {@code instance.id} (default {@code connector.name::hostname}).
 * It forms the YDB {@code producer_id} and {@code message_group_id} together with the mapped
 * destination, so partition writer sessions do not collide across replicas.
 */
@Named("ydb")
@Dependent
public class YdbChangeConsumer extends BaseChangeConsumer
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(YdbChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.ydb.";

    private static final String KEY_METADATA_ITEM = "key";

    private static final String DEFAULT_GROUP = "__default";

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    private YdbChangeConsumerConfig config;
    private GrpcTransport transport;
    private TopicClient topicClient;

    private final ConcurrentMap<WriterKey, AsyncWriter> writers = new ConcurrentHashMap<>();

    private final Set<String> verifiedTopicPaths = ConcurrentHashMap.newKeySet();

    @PostConstruct
    void connect() {
        config = new YdbChangeConsumerConfig(
                Configuration.from(getConfigSubset(ConfigProvider.getConfig(), PROP_PREFIX)));

        try {
            YdbTransport.Clients clients = YdbTransport.open(config);
            transport = clients.transport();
            topicClient = clients.topicClient();
        }
        catch (Exception e) {
            throw new DebeziumException("Failed to initialize YDB transport/topic client", e);
        }

        LOGGER.info("YdbChangeConsumer initialized: endpoint={} database={} instanceId={} codec={} topicAutoCreate={}",
                config.getEndpoint(), config.getDatabase(), config.getResolvedInstanceId(),
                config.getProducerCodec(), config.isTopicAutoCreate());
    }

    @PreDestroy
    @Override
    public void close() {
        for (Map.Entry<WriterKey, AsyncWriter> e : writers.entrySet()) {
            try {
                e.getValue().shutdown().get(config.getWriterShutdownTimeoutMs(), TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOGGER.warn("Interrupted while shutting down YDB writer for {}", e.getKey());
                break;
            }
            catch (Exception ex) {
                LOGGER.warn("Exception while shutting down YDB writer for {}", e.getKey(), ex);
            }
        }
        writers.clear();
        if (topicClient != null) {
            try {
                topicClient.close();
            }
            catch (Exception e) {
                LOGGER.warn("Exception while closing YDB TopicClient", e);
            }
        }
        if (transport != null) {
            try {
                transport.close();
            }
            catch (Exception e) {
                LOGGER.warn("Exception while closing YDB GrpcTransport", e);
            }
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records,
                            RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        if (records.isEmpty()) {
            committer.markBatchFinished();
            return;
        }

        List<CompletableFuture<WriteAck>> acks = new ArrayList<>(records.size());

        for (ChangeEvent<Object, Object> record : records) {
            if (record.value() == null) {
                continue;
            }

            String destination = streamNameMapper.map(record.destination());
            String topicPath = resolveTopicPath(destination);
            String group = destination == null ? DEFAULT_GROUP : destination;

            AsyncWriter writer = writerFor(topicPath, group);

            try {
                acks.add(writer.send(buildMessage(record)));
            }
            catch (Exception e) {
                throw new DebeziumException("Failed to enqueue message into YDB topic " + topicPath, e);
            }
        }

        waitForWriteAcks(acks);

        for (ChangeEvent<Object, Object> record : records) {
            committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }

    private Message buildMessage(ChangeEvent<Object, Object> record) {
        Message.Builder builder = Message.newBuilder().setData(getBytes(record.value()));
        if (record.key() != null) {
            builder.addMetadataItem(new MetadataItem(KEY_METADATA_ITEM, getBytes(record.key())));
        }
        return builder.build();
    }

    @Override
    public Field.Set getConfigFields() {
        return YdbChangeConsumerConfig.ALL_FIELDS;
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }

    private String resolveTopicPath(String destination) {
        return resolveTopicPath(config.getTopic(), config.getTopicPrefix(), destination);
    }

    static String resolveTopicPath(String topic, String prefix, String destination) {
        if (!Strings.isNullOrBlank(topic)) {
            return topic;
        }
        if (destination == null) {
            throw new DebeziumException("YDB sink: record has no destination and 'topic' is not configured");
        }
        if (Strings.isNullOrBlank(prefix)) {
            return destination;
        }
        boolean prefixEndsSlash = prefix.endsWith("/");
        boolean destStartsSlash = destination.startsWith("/");
        if (prefixEndsSlash && destStartsSlash) {
            return prefix + destination.substring(1);
        }
        if (prefixEndsSlash || destStartsSlash) {
            return prefix + destination;
        }
        return prefix + "/" + destination;
    }

    private AsyncWriter writerFor(String topicPath, String group) {
        return writers.computeIfAbsent(new WriterKey(topicPath, group), this::openWriter);
    }

    private void waitForWriteAcks(List<CompletableFuture<WriteAck>> acks) throws InterruptedException {
        if (acks.isEmpty()) {
            return;
        }
        long timeoutMs = config.getWriterAckTimeoutMs();
        try {
            CompletableFuture.allOf(acks.toArray(CompletableFuture[]::new))
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
        catch (TimeoutException e) {
            throw new DebeziumException(
                    "Timed out waiting for YDB topic write acks after " + timeoutMs + " ms", e);
        }
        catch (ExecutionException e) {
            propagateAsyncFailure(e);
        }
    }

    static void propagateAsyncFailure(Throwable failure) throws InterruptedException {
        Throwable cause = unwrapAsyncFailure(failure);
        if (cause instanceof InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        }
        if (cause instanceof CancellationException ce) {
            Thread.currentThread().interrupt();
            InterruptedException ie = new InterruptedException("YDB operation cancelled");
            ie.initCause(ce);
            throw ie;
        }
        if (failure instanceof DebeziumException de) {
            throw de;
        }
        throw new DebeziumException("YDB operation failed", cause);
    }

    static Throwable unwrapAsyncFailure(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException || current instanceof ExecutionException) {
            Throwable cause = current.getCause();
            if (cause == null) {
                break;
            }
            current = cause;
        }
        return current;
    }

    private AsyncWriter openWriter(WriterKey key) {
        ensureTopicIfNeeded(key.topicPath());

        String producerId = config.getResolvedInstanceId() + "::" + key.group();
        WriterSettings ws = WriterSettings.newBuilder()
                .setTopicPath(key.topicPath())
                .setProducerId(producerId)
                .setMessageGroupId(producerId)
                .setCodec(config.getProducerCodec())
                .build();
        AsyncWriter w = topicClient.createAsyncWriter(ws);
        w.init().join();
        LOGGER.info("Opened YDB AsyncWriter producerId={} topic={}", producerId, key.topicPath());
        return w;
    }

    private void ensureTopicIfNeeded(String topicPath) {
        if (!config.isTopicAutoCreate()) {
            return;
        }
        if (verifiedTopicPaths.add(topicPath)) {
            try {
                createTopicIfMissing(topicPath);
            }
            catch (RuntimeException e) {
                verifiedTopicPaths.remove(topicPath);
                throw e;
            }
        }
    }

    private void createTopicIfMissing(String topicPath) {
        Result<TopicDescription> described = topicClient.describeTopic(topicPath).join();
        if (described.isSuccess()) {
            return;
        }
        StatusCode code = described.getStatus().getCode();
        if (code != StatusCode.SCHEME_ERROR && code != StatusCode.NOT_FOUND) {
            described.getStatus().expectSuccess("describe topic " + topicPath);
        }

        Status created = topicClient.createTopic(topicPath, buildAutoCreateTopicSettings()).join();
        if (created.isSuccess()) {
            LOGGER.info("Auto-created YDB topic {}", topicPath);
            return;
        }
        if (created.getCode() == StatusCode.ALREADY_EXISTS) {
            return;
        }
        created.expectSuccess("create topic " + topicPath);
    }

    private CreateTopicSettings buildAutoCreateTopicSettings() {
        long partitions = config.getTopicAutoCreateMinActivePartitions();
        PartitioningSettings partitioning = PartitioningSettings.newBuilder()
                .setMinActivePartitions(partitions)
                .setMaxActivePartitions(partitions)
                .build();

        SupportedCodecs codecs = SupportedCodecs.newBuilder()
                .addCodec(Codec.RAW)
                .addCodec(Codec.GZIP)
                .addCodec(Codec.ZSTD)
                .build();

        CreateTopicSettings.Builder builder = CreateTopicSettings.newBuilder()
                .setPartitioningSettings(partitioning)
                .setSupportedCodecs(codecs);

        String consumerName = config.getTopicAutoCreateInitialConsumer();
        if (!Strings.isNullOrBlank(consumerName)) {
            builder.addConsumer(Consumer.newBuilder().setName(consumerName.trim()).build());
        }

        return builder.build();
    }

    private record WriterKey(String topicPath, String group) {
    }
}
