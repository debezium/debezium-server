/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.Module;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.ConnectionValidationResult;
import io.debezium.server.CustomConsumerBuilder;
import io.debezium.server.api.DebeziumServerSink;

/**
 * Native Debezium Server sink that maps change events to ZeroBus request records.
 */
@Named("zerobus")
@Dependent
public class ZeroBusChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroBusChangeConsumer.class);
    private static final String PROP_PREFIX = "debezium.sink.zerobus.";

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    private ZeroBusClient client;
    private ZeroBusBatchWriter writer;
    private ZeroBusSinkConfig config;

    @Inject
    @CustomConsumerBuilder
    Instance<ZeroBusClient> customClient;

    @PostConstruct
    void connect() {
        initWithConfig(ConfigProvider.getConfig(), null);
    }

    @VisibleForTesting
    void initWithConfig(Config mpConfig, ZeroBusClient clientOverride) {
        Configuration configuration = Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));
        config = new ZeroBusSinkConfig(configuration);
        config.validate();

        client = clientOverride != null ? clientOverride : resolveClient();
        ZeroBusTableRouter router = new ZeroBusTableRouter(config);
        ZeroBusRecordMapper mapper = new ZeroBusRecordMapper(router, config);
        writer = new ZeroBusBatchWriter(config, client, mapper);

        LOGGER.info("ZeroBus sink configured for endpoint '{}' with table mapping mode '{}'",
                config.getEndpoint(), config.getTableMappingMode());
    }

    private ZeroBusClient resolveClient() {
        if (customClient != null && customClient.isResolvable()) {
            return customClient.get();
        }
        return new DatabricksZeroBusClient(config);
    }

    @PreDestroy
    @Override
    public void close() {
        if (client != null) {
            try {
                client.close();
            }
            catch (Exception e) {
                LOGGER.warn("Exception while closing ZeroBus client", e);
            }
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records,
                            DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        if (records.isEmpty()) {
            committer.markBatchFinished();
            return;
        }

        List<ChangeEvent<Object, Object>> processedRecords = writer.write(records);
        for (ChangeEvent<Object, Object> record : processedRecords) {
            committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }

    @Override
    public ConnectionValidationResult validateConnection(Map<String, Object> config) {
        try {
            ZeroBusSinkConfig sinkConfig = new ZeroBusSinkConfig(Configuration.from(config));
            sinkConfig.validate();
            return ConnectionValidationResult.successful();
        }
        catch (Exception e) {
            return ConnectionValidationResult.failed(e.getMessage());
        }
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                ZeroBusSinkConfig.ENDPOINT,
                ZeroBusSinkConfig.WORKSPACE_URL,
                ZeroBusSinkConfig.AUTHENTICATION_TYPE,
                ZeroBusSinkConfig.OAUTH2_CLIENT_ID,
                ZeroBusSinkConfig.OAUTH2_CLIENT_SECRET,
                ZeroBusSinkConfig.RECORD_FORMAT,
                ZeroBusSinkConfig.TABLE_MAPPING_MODE,
                ZeroBusSinkConfig.TABLE_MAPPING_DEFAULT_CATALOG,
                ZeroBusSinkConfig.TABLE_MAPPING_DEFAULT_SCHEMA,
                ZeroBusSinkConfig.TABLE_MAPPING_OVERRIDES,
                ZeroBusSinkConfig.TABLE_MAPPING_REGEX,
                ZeroBusSinkConfig.TABLE_MAPPING_REPLACEMENT,
                ZeroBusSinkConfig.BATCH_SIZE,
                ZeroBusSinkConfig.RETRIES,
                ZeroBusSinkConfig.RETRY_INTERVAL_MS,
                ZeroBusSinkConfig.TIMEOUT_MS,
                ZeroBusSinkConfig.MAX_INFLIGHT_RECORDS,
                ZeroBusSinkConfig.MAX_INFLIGHT_BATCHES,
                ZeroBusSinkConfig.IDEMPOTENCY_MODE,
                ZeroBusSinkConfig.TOMBSTONE_HANDLING_MODE);
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }
}
