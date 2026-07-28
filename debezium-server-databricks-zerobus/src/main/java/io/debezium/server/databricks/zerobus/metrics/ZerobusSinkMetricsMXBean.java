/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus.metrics;

/**
 * Metrics exposed by the Databricks Zerobus sink over JMX.
 * <p>
 * They are registered under the object name
 * {@code debezium.zerobus:type=connector-metrics,context=sink,server=<route>,task=0} — mirroring the
 * convention used by the JDBC sink connector ({@code JdbcSinkConnectorMetricsMXBean}) and by Debezium
 * source connectors. The {@code server} key carries the delivery route ({@code grpc} or {@code rest})
 * so both consumers can be observed side by side.
 * <p>
 * The counters describe what the sink actually forwarded to Zerobus (the target managed Delta
 * table): how many change events were ingested, split by operation ({@code c}/{@code u}/{@code d} and
 * snapshot reads {@code r}), how many were skipped (tombstones / non-qualified destinations such as
 * schema-history or DDL events), and how many failed. In addition to throughput, they expose the
 * end-to-end freshness of the pipeline — {@link #getMilliSecondsBehindSource()} — which is the
 * primary question in a CDC → lakehouse deployment: how far behind the source database the Delta
 * table currently is.
 */
public interface ZerobusSinkMetricsMXBean {

    String METRIC_TOTAL_RECORDS_INGESTED = "TotalRecordsIngested";
    String METRIC_TOTAL_RECORDS_SKIPPED = "TotalRecordsSkipped";
    String METRIC_TOTAL_ERRORS = "TotalErrors";
    String METRIC_TOTAL_FLUSHES = "TotalFlushes";
    String METRIC_TOTAL_INSERTS = "TotalInserts";
    String METRIC_TOTAL_UPDATES = "TotalUpdates";
    String METRIC_TOTAL_DELETES = "TotalDeletes";
    String METRIC_TOTAL_READS = "TotalReads";
    String METRIC_ACTIVE_STREAMS = "ActiveStreams";
    String METRIC_MILLISECONDS_BEHIND_SOURCE = "MilliSecondsBehindSource";
    String METRIC_LAST_FLUSH_DURATION_MILLIS = "LastFlushDurationMillis";
    String METRIC_MAX_FLUSH_DURATION_MILLIS = "MaxFlushDurationMillis";
    String METRIC_ROUTE = "Route";

    /**
     * @return the total number of change events forwarded to Zerobus (across all tables/streams)
     */
    long getTotalRecordsIngested();

    /**
     * @return the total number of records skipped without being forwarded (tombstones / null
     *         payloads, and events whose destination is not a fully qualified {@code catalog.schema.table})
     */
    long getTotalRecordsSkipped();

    /**
     * @return the total number of ingestion or flush failures observed
     */
    long getTotalErrors();

    /**
     * @return the total number of stream flushes performed (gRPC route); for the REST route this
     *         tracks successful record POSTs, which are the durability boundary there
     */
    long getTotalFlushes();

    /**
     * @return the total number of create ({@code op=c}) change events ingested
     */
    long getTotalInserts();

    /**
     * @return the total number of update ({@code op=u}) change events ingested
     */
    long getTotalUpdates();

    /**
     * @return the total number of delete ({@code op=d}) change events ingested
     */
    long getTotalDeletes();

    /**
     * @return the total number of snapshot read ({@code op=r}) events ingested
     */
    long getTotalReads();

    /**
     * @return the number of currently open Zerobus streams (one per target table); always {@code 0}
     *         for the REST route, which opens no persistent streams
     */
    long getActiveStreams();

    /**
     * @return the pipeline freshness: {@code now - source.ts_ms} of the most recently ingested
     *         event, in milliseconds; {@code -1} until the first event with a source timestamp is
     *         ingested. Approaches {@code 0} when the sink is caught up with the source.
     */
    long getMilliSecondsBehindSource();

    /**
     * @return the duration of the most recent flush (gRPC) or record POST (REST), in milliseconds
     */
    long getLastFlushDurationMillis();

    /**
     * @return the longest flush/POST duration observed, in milliseconds
     */
    long getMaxFlushDurationMillis();

    /**
     * @return the delivery route these metrics belong to ({@code grpc} or {@code rest})
     */
    String getRoute();
}
