/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.infinispan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
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
import io.debezium.server.api.DebeziumServerSink;

/**
 * An implementation of the {@link DebeziumEngine.ChangeConsumer} interface that publishes change event messages to predefined Infinispan cache.
 *
 * @author vjuranek
 */
@Named("infinispan")
@Dependent
public class InfinispanSinkConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(InfinispanSinkConsumer.class);

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    private static final String CONF_PREFIX = "debezium.sink.infinispan.";

    private InfinispanSinkConsumerConfig config;
    private RemoteCacheManager remoteCacheManager;
    private RemoteCache cache;

    @Inject
    @CustomConsumerBuilder
    Instance<RemoteCache> customCache;

    @PostConstruct
    void connect() {
        if (customCache.isResolvable()) {
            cache = customCache.get();
            LOGGER.info("Obtained custom cache with configuration '{}'", cache.getRemoteCacheContainer().getConfiguration());
            return;
        }

        final Config mpConfig = ConfigProvider.getConfig();

        // Load configuration
        io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, CONF_PREFIX));
        this.config = new InfinispanSinkConsumerConfig(configuration);

        final String serverHost = config.getServerHost();
        final String cacheName = config.getCacheName();
        final Integer serverPort = config.getServerPort() != null ? config.getServerPort() : ConfigurationProperties.DEFAULT_HOTROD_PORT;

        ConfigurationBuilder builder = new ConfigurationBuilder();
        String uri;
        if (config.getUser() != null && config.getPassword() != null) {
            uri = String.format("hotrod://%s:%s@%s:%d", config.getUser(), config.getPassword(), serverHost, serverPort);
        }
        else {
            uri = String.format("hotrod://%s:%d", serverHost, serverPort);
        }
        LOGGER.info("Connecting to the Infinispan server using URI '{}'", uri);
        builder.uri(uri);

        remoteCacheManager = new RemoteCacheManager(builder.build());
        cache = remoteCacheManager.getCache(cacheName);
        LOGGER.info("Connected to the Infinispan server {}", remoteCacheManager.getServers()[0]);
    }

    @PreDestroy
    @Override
    public void close() {
        try {
            if (remoteCacheManager != null) {
                remoteCacheManager.close();
                LOGGER.info("Connection to Infinispan server closed.");
            }
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        Map<Object, Object> entries = new HashMap<>(records.size());
        for (ChangeEvent<Object, Object> record : records) {
            if (record.value() != null) {
                LOGGER.trace("Received event {} = '{}'", getString(record.key()), getString(record.value()));
                entries.put(record.key(), record.value());
            }
        }

        try {
            cache.putAll(entries);
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }

        for (ChangeEvent<Object, Object> record : records) {
            committer.markProcessed(record);
        }

        committer.markBatchFinished();
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                InfinispanSinkConsumerConfig.SERVER_HOST,
                InfinispanSinkConsumerConfig.SERVER_PORT,
                InfinispanSinkConsumerConfig.CACHE,
                InfinispanSinkConsumerConfig.USER,
                InfinispanSinkConsumerConfig.PASSWORD);
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }
}
