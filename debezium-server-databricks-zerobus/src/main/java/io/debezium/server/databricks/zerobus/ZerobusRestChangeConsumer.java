/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.config.Field;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataFactory;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.api.DebeziumServerConsumer;
import io.debezium.server.api.DebeziumServerSink;
import io.debezium.server.databricks.zerobus.metrics.ZerobusSinkMetrics;

/**
 * Debezium Server sink that writes change events into Databricks Zerobus Ingest over the REST API
 * ({@code POST <uri>/zerobus/v1/tables/<catalog.schema.table>/insert}). Events are serialized as
 * flat JSON by Debezium and posted one per request with an OAuth bearer token.
 * <p>
 * This route needs neither the Kafka ingress flag nor a persistent connection, so it suits
 * serverless / edge deployments. Like the other routes it is at-least-once; deduplicate downstream.
 */
@Named("zerobusrest")
@Dependent
public class ZerobusRestChangeConsumer extends BaseChangeConsumer
        implements DebeziumServerConsumer<CapturingEvents<BatchEvent>>, DebeziumServerSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZerobusRestChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.zerobusrest.";

    private final ComponentMetadataFactory componentMetadataFactory = new ComponentMetadataFactory();

    private ZerobusRestChangeConsumerConfig config;
    private ZerobusTokenProvider tokenProvider;
    private HttpClient httpClient;
    private String baseUri;
    private final ZerobusSinkMetrics metrics = new ZerobusSinkMetrics("rest");
    private long batchesSinceLog = 0;

    @PostConstruct
    void connect() {
        final Config mpConfig = ConfigProvider.getConfig();
        io.debezium.config.Configuration configuration = io.debezium.config.Configuration.from(getConfigSubset(mpConfig, PROP_PREFIX));
        this.config = new ZerobusRestChangeConsumerConfig(configuration);

        this.baseUri = config.getUri().endsWith("/") ? config.getUri().substring(0, config.getUri().length() - 1) : config.getUri();
        this.tokenProvider = new ZerobusTokenProvider(
                config.getWorkspaceUrl(), config.getWorkspaceId(), config.getClientId(), config.getClientSecret(), config.getTable());
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

        metrics.register();
        LOGGER.info("Zerobus REST sink connected: uri={}, table={}", baseUri, config.getTable());
    }

    @PreDestroy
    @Override
    public void close() {
        metrics.unregister();
        LOGGER.info("Zerobus REST sink closed");
    }

    @Override
    public void handle(CapturingEvents<BatchEvent> events) throws InterruptedException {
        for (BatchEvent record : events.records()) {
            String json = ZerobusChangeConsumer.toZerobusJson(record, this::getString);
            String table = resolveTable(record.destination());
            if (ZerobusChangeConsumer.isJsonObject(json) && table != null) {
                try {
                    final long postStartNanos = System.nanoTime();
                    post(table, json);
                    metrics.flushed((System.nanoTime() - postStartNanos) / 1_000_000L);
                    metrics.recordIngested(ZerobusChangeConsumer.operationOf(record), ZerobusChangeConsumer.sourceTsMsOf(record));
                }
                catch (IOException e) {
                    metrics.recordError();
                    throw new DebeziumException("Failed to POST record to Zerobus REST for table '" + table + "'", e);
                }
            }
            else {
                metrics.recordSkipped();
            }
            record.commit();
        }
        maybeLogMetrics();
    }

    private void maybeLogMetrics() {
        final int interval = config.getMetricsLogInterval();
        if (interval > 0 && ++batchesSinceLog >= interval) {
            batchesSinceLog = 0;
            metrics.logMetricsSummary();
        }
    }

    private void post(String table, String json) throws IOException, InterruptedException {
        String url = baseUri + "/zerobus/v1/tables/" + table + "/insert";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + tokenProvider.currentToken())
                .header("unity-catalog-endpoint", config.getWorkspaceUrl())
                .header("x-databricks-zerobus-table-name", table)
                .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8))
                .build();

        HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            throw new IOException("Zerobus REST returned HTTP " + resp.statusCode() + " for table '" + table + "': " + resp.body());
        }
        LOGGER.trace("Ingested record to {} (HTTP {})", table, resp.statusCode());
    }

    private String resolveTable(String destination) {
        String configured = config.getTable();
        if (configured != null && !configured.isBlank()) {
            return configured;
        }
        String mapped = streamNameMapper.map(destination);
        return ZerobusChangeConsumer.isQualifiedTable(mapped) ? mapped : null;
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                ZerobusRestChangeConsumerConfig.URI,
                ZerobusRestChangeConsumerConfig.WORKSPACE_URL,
                ZerobusRestChangeConsumerConfig.WORKSPACE_ID,
                ZerobusRestChangeConsumerConfig.CLIENT_ID,
                ZerobusRestChangeConsumerConfig.CLIENT_SECRET,
                ZerobusRestChangeConsumerConfig.TABLE,
                ZerobusRestChangeConsumerConfig.METRICS_LOG_INTERVAL);
    }

    @Override
    public List<ComponentMetadata> getConnectorMetadata() {
        return List.of(componentMetadataFactory.createComponentMetadata(this, Module.version()));
    }
}
