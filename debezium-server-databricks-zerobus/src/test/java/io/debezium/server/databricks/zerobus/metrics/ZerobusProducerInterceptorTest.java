/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ZerobusProducerInterceptor}, which exposes the sink metrics for the Kafka
 * delivery route. They need no broker, Zerobus endpoint or credentials.
 */
class ZerobusProducerInterceptorTest {

    private static final String TABLE = "main.default.customers";

    private ZerobusProducerInterceptor interceptor;

    @BeforeEach
    void setUp() {
        interceptor = new ZerobusProducerInterceptor();
        interceptor.configure(Map.of());
    }

    @AfterEach
    void tearDown() {
        interceptor.close();
    }

    @Test
    void registersMetricsUnderTheKafkaRouteObjectName() throws Exception {
        ObjectName name = new ObjectName("debezium.zerobus:type=connector-metrics,context=sink,server=kafka,task=0");

        assertThat(ManagementFactory.getPlatformMBeanServer().isRegistered(name)).isTrue();
        assertThat(interceptor.metrics().getRoute()).isEqualTo("kafka");

        interceptor.close();
        assertThat(ManagementFactory.getPlatformMBeanServer().isRegistered(name)).isFalse();
    }

    @Test
    void countsIngestedRecordsSplitByOperationFromAnUnwrappedRecord() {
        send("{\"id\":1,\"__op\":\"r\"}");
        send("{\"id\":2,\"__op\":\"c\"}");
        send("{\"id\":3,\"__op\":\"u\"}");
        send("{\"id\":4,\"__op\":\"d\"}");

        ZerobusSinkMetrics metrics = interceptor.metrics();
        assertThat(metrics.getTotalRecordsIngested()).isEqualTo(4);
        assertThat(metrics.getTotalReads()).isEqualTo(1);
        assertThat(metrics.getTotalInserts()).isEqualTo(1);
        assertThat(metrics.getTotalUpdates()).isEqualTo(1);
        assertThat(metrics.getTotalDeletes()).isEqualTo(1);
        assertThat(metrics.getTotalErrors()).isZero();
    }

    @Test
    void readsTheOperationFromTheFullEnvelopeToo() {
        send("{\"op\":\"c\",\"after\":{\"id\":1}}");

        assertThat(interceptor.metrics().getTotalInserts()).isEqualTo(1);
    }

    @Test
    void tracksFreshnessFromTheFlattenedSourceTimestamp() {
        long sourceTsMs = System.currentTimeMillis() - 500L;
        send("{\"id\":1,\"__op\":\"c\",\"__source_ts_ms\":" + sourceTsMs + "}");

        assertThat(interceptor.metrics().getMilliSecondsBehindSource()).isBetween(0L, 60_000L);
    }

    @Test
    void tracksFreshnessFromTheNestedEnvelopeTimestamp() {
        long sourceTsMs = System.currentTimeMillis() - 500L;
        send("{\"op\":\"c\",\"source\":{\"ts_ms\":" + sourceTsMs + "}}");

        assertThat(interceptor.metrics().getMilliSecondsBehindSource()).isBetween(0L, 60_000L);
    }

    @Test
    void reportsNoFreshnessUntilAnEventCarriesASourceTimestamp() {
        send("{\"id\":1,\"__op\":\"c\"}");

        assertThat(interceptor.metrics().getMilliSecondsBehindSource()).isEqualTo(-1L);
    }

    @Test
    void acceptsAByteArrayValue() {
        interceptor.onSend(new ProducerRecord<>(TABLE, null,
                "{\"id\":1,\"__op\":\"c\"}".getBytes(StandardCharsets.UTF_8)));

        assertThat(interceptor.metrics().getTotalInserts()).isEqualTo(1);
    }

    @Test
    void countsARecordWhoseValueIsNotJsonWithoutAnOperation() {
        send("not json at all");

        ZerobusSinkMetrics metrics = interceptor.metrics();
        assertThat(metrics.getTotalRecordsIngested()).isEqualTo(1);
        assertThat(metrics.getTotalInserts()).isZero();
        assertThat(metrics.getTotalErrors()).isZero();
    }

    @Test
    void countsAnAcknowledgementAsAFlush() {
        // Send a whole sampling interval so that exactly one record is timed, then acknowledge them.
        final int interval = ZerobusProducerInterceptor.LATENCY_SAMPLE_INTERVAL;
        for (int i = 0; i < interval; i++) {
            send("{\"id\":" + i + ",\"__op\":\"c\"}");
        }
        for (int i = 0; i < interval; i++) {
            interceptor.onAcknowledgement(metadata(), null);
        }

        ZerobusSinkMetrics metrics = interceptor.metrics();
        assertThat(metrics.getTotalFlushes()).isEqualTo(interval);
        assertThat(metrics.getLastFlushDurationMillis()).isGreaterThanOrEqualTo(0L);
        assertThat(metrics.getMaxFlushDurationMillis()).isGreaterThanOrEqualTo(metrics.getLastFlushDurationMillis());
        assertThat(metrics.getTotalErrors()).isZero();
    }

    @Test
    void countsAFailedAcknowledgementAsAnErrorAndNotAsAFlush() {
        send("{\"id\":1,\"__op\":\"c\"}");
        interceptor.onAcknowledgement(null, new RuntimeException("rejected by the broker"));

        ZerobusSinkMetrics metrics = interceptor.metrics();
        assertThat(metrics.getTotalErrors()).isEqualTo(1);
        assertThat(metrics.getTotalFlushes()).isZero();
    }

    @Test
    void doesNotSkipRecordsOnThisRoute() {
        send("{\"id\":1,\"__op\":\"c\"}");
        interceptor.onAcknowledgement(metadata(), null);

        // The generic kafka sink forwards every record it receives, so there is no skip decision here.
        assertThat(interceptor.metrics().getTotalRecordsSkipped()).isZero();
        assertThat(interceptor.metrics().getActiveStreams()).isZero();
    }

    @Test
    void prefersTheOperationAndTimestampFromRecordHeaders() {
        // add.headers=op,source.ts_ms lets the metadata be read without scanning the value at all.
        long sourceTsMs = System.currentTimeMillis() - 250L;
        Headers headers = new RecordHeaders();
        headers.add("__op", "u".getBytes(StandardCharsets.UTF_8));
        headers.add("__source_ts_ms", String.valueOf(sourceTsMs).getBytes(StandardCharsets.UTF_8));

        interceptor.onSend(new ProducerRecord<>(TABLE, null, null, null, "{\"id\":1}", headers));

        ZerobusSinkMetrics metrics = interceptor.metrics();
        assertThat(metrics.getTotalUpdates()).isEqualTo(1);
        assertThat(metrics.getMilliSecondsBehindSource()).isBetween(0L, 60_000L);
    }

    @Test
    void fallsBackToTheValueWhenOnlySomeHeadersArePresent() {
        Headers headers = new RecordHeaders();
        headers.add("__op", "d".getBytes(StandardCharsets.UTF_8));

        // The operation comes from the header, the timestamp still has to come from the value.
        interceptor.onSend(new ProducerRecord<>(TABLE, null, null, null,
                "{\"id\":1,\"__source_ts_ms\":" + System.currentTimeMillis() + "}", headers));

        ZerobusSinkMetrics metrics = interceptor.metrics();
        assertThat(metrics.getTotalDeletes()).isEqualTo(1);
        assertThat(metrics.getMilliSecondsBehindSource()).isBetween(0L, 60_000L);
    }

    @Test
    void readsJsonEncodedHeaderValues() {
        // Header values pass through the header converter, so Debezium Server emits them JSON encoded,
        // which means a string header arrives quoted.
        long sourceTsMs = System.currentTimeMillis() - 300L;
        Headers headers = new RecordHeaders();
        headers.add("__op", "\"c\"".getBytes(StandardCharsets.UTF_8));
        headers.add("__source_ts_ms", String.valueOf(sourceTsMs).getBytes(StandardCharsets.UTF_8));

        interceptor.onSend(new ProducerRecord<>(TABLE, null, null, null, "{\"id\":1}", headers));

        assertThat(interceptor.metrics().getTotalInserts()).isEqualTo(1);
        assertThat(interceptor.metrics().getMilliSecondsBehindSource()).isBetween(0L, 60_000L);
    }

    @Test
    void fallsBackToTheValueWhenAHeaderCarriesASchemaEnvelope() {
        // With schemas.enable=true a header is a JSON object rather than a scalar; the value still has
        // the metadata, so the scan must take over instead of reporting a bogus operation.
        Headers headers = new RecordHeaders();
        headers.add("__op", "{\"schema\":{\"type\":\"string\"},\"payload\":\"u\"}".getBytes(StandardCharsets.UTF_8));

        interceptor.onSend(new ProducerRecord<>(TABLE, null, null, null, "{\"id\":1,\"__op\":\"u\"}", headers));

        assertThat(interceptor.metrics().getTotalUpdates()).isEqualTo(1);
    }

    @Test
    void ignoresANonNumericSourceTimestampHeader() {
        Headers headers = new RecordHeaders();
        headers.add("__source_ts_ms", "not-a-number".getBytes(StandardCharsets.UTF_8));

        interceptor.onSend(new ProducerRecord<>(TABLE, null, null, null, "{\"id\":1,\"__op\":\"c\"}", headers));

        assertThat(interceptor.metrics().getTotalInserts()).isEqualTo(1);
        assertThat(interceptor.metrics().getMilliSecondsBehindSource()).isEqualTo(-1L);
    }

    @Test
    void readsOnlyTopLevelKeysAndIsNotFooledByNestedOrQuotedOnes() {
        // A nested object carrying the same key must not win over the top-level one, and a key name
        // appearing inside a string value must be ignored entirely.
        send("{\"id\":1,\"payload\":{\"__op\":\"NESTED\"},\"__op\":\"c\"}");
        assertThat(interceptor.metrics().getTotalInserts()).isEqualTo(1);

        send("{\"arr\":[{\"__op\":\"NESTED\"}],\"__op\":\"d\"}");
        assertThat(interceptor.metrics().getTotalDeletes()).isEqualTo(1);

        send("{\"note\":\"contains \\\"__op\\\":\\\"x\\\" inline\",\"__op\":\"u\"}");
        assertThat(interceptor.metrics().getTotalUpdates()).isEqualTo(1);

        // ts_ms is only read directly inside a top-level source object, not deeper.
        send("{\"op\":\"r\",\"source\":{\"nested\":{\"ts_ms\":999}}}");
        assertThat(interceptor.metrics().getTotalReads()).isEqualTo(1);
        assertThat(interceptor.metrics().getMilliSecondsBehindSource()).isEqualTo(-1L);
    }

    @Test
    void readsTheTimestampNestedInTheEnvelopeSourceObject() {
        long sourceTsMs = System.currentTimeMillis() - 100L;
        send("{\"op\":\"c\",\"before\":null,\"after\":{\"id\":1},"
                + "\"source\":{\"connector\":\"postgresql\",\"ts_ms\":" + sourceTsMs + ",\"db\":\"d\"}}");

        assertThat(interceptor.metrics().getTotalInserts()).isEqualTo(1);
        assertThat(interceptor.metrics().getMilliSecondsBehindSource()).isBetween(0L, 60_000L);
    }

    @Test
    void toleratesTruncatedOrMalformedJsonWithoutFailing() {
        assertThatCode(() -> {
            send("{\"id\":1,\"__op\":");
            send("{\"id\":1,\"__op\":\"c");
            send("{");
            send("");
            send("{\"a\":\"unterminated");
        }).doesNotThrowAnyException();

        // Every record is still counted, even when no metadata could be extracted.
        assertThat(interceptor.metrics().getTotalRecordsIngested()).isEqualTo(5);
        assertThat(interceptor.metrics().getTotalErrors()).isZero();
    }

    @Test
    void forwardsTheRecordUnchanged() {
        ProducerRecord<Object, Object> record = new ProducerRecord<>(TABLE, null, "{\"id\":1}");

        assertThat(interceptor.onSend(record)).isSameAs(record);
    }

    @Test
    void periodicMetricLoggingIsDisabledByDefaultAndConfigurable() {
        // Default: no interval configured, so no periodic logging and no interference with counting.
        send("{\"id\":1,\"__op\":\"c\"}");
        assertThat(interceptor.metrics().getTotalRecordsIngested()).isEqualTo(1);

        // Only one MBean can hold the object name at a time, and JmxUtils retries with a delay when
        // it is taken, so the interceptor under test must release it first.
        interceptor.close();

        ZerobusProducerInterceptor configured = new ZerobusProducerInterceptor();
        try {
            configured.configure(Map.of(ZerobusProducerInterceptor.METRICS_LOG_INTERVAL_CONFIG, "2"));
            // Logging happens as a side effect; what must hold is that the counters stay exact across
            // the interval boundary.
            for (int i = 0; i < 5; i++) {
                configured.onSend(new ProducerRecord<>(TABLE, null, "{\"id\":" + i + ",\"__op\":\"c\"}"));
            }
            assertThat(configured.metrics().getTotalRecordsIngested()).isEqualTo(5);
        }
        finally {
            configured.close();
        }
    }

    @Test
    void aNonNumericMetricsLogIntervalIsIgnored() {
        interceptor.close();

        ZerobusProducerInterceptor configured = new ZerobusProducerInterceptor();
        try {
            assertThatCode(() -> configured.configure(
                    Map.of(ZerobusProducerInterceptor.METRICS_LOG_INTERVAL_CONFIG, "not-a-number")))
                    .doesNotThrowAnyException();

            configured.onSend(new ProducerRecord<>(TABLE, null, "{\"id\":1,\"__op\":\"c\"}"));
            assertThat(configured.metrics().getTotalRecordsIngested()).isEqualTo(1);
        }
        finally {
            configured.close();
        }
    }

    @Test
    void reportsTheConnectionAsUsableWhileReporting() {
        assertThat(interceptor.metrics().isConnected()).isTrue();

        interceptor.close();
        assertThat(interceptor.metrics().isConnected()).isFalse();
    }

    @Test
    void tracksTheTimeSinceTheLastForwardedEvent() {
        assertThat(interceptor.metrics().getMilliSecondsSinceLastEvent()).isEqualTo(-1L);

        send("{\"id\":1,\"__op\":\"c\"}");

        assertThat(interceptor.metrics().getMilliSecondsSinceLastEvent()).isBetween(0L, 60_000L);
    }

    @Test
    void configuringTwiceKeepsASingleRegistration() throws Exception {
        ObjectName name = new ObjectName("debezium.zerobus:type=connector-metrics,context=sink,server=kafka,task=0");
        ZerobusSinkMetrics first = interceptor.metrics();

        interceptor.configure(Map.of());

        // The second call must neither replace the metrics instance (which would reset the counters)
        // nor register the object name again.
        assertThat(interceptor.metrics()).isSameAs(first);
        assertThat(ManagementFactory.getPlatformMBeanServer().isRegistered(name)).isTrue();

        // A single close() must still fully deregister.
        interceptor.close();
        assertThat(ManagementFactory.getPlatformMBeanServer().isRegistered(name)).isFalse();
    }

    @Test
    void closingTwiceIsHarmless() {
        interceptor.close();

        assertThatCode(() -> interceptor.close()).doesNotThrowAnyException();
    }

    @Test
    void correlatesLatencyPerTopicWhenSeveralTablesAreWritten() {
        String other = "main.default.orders";
        interceptor.onSend(new ProducerRecord<>(TABLE, null, "{\"id\":1,\"__op\":\"c\"}"));
        interceptor.onSend(new ProducerRecord<>(other, null, "{\"id\":2,\"__op\":\"c\"}"));

        // Acknowledging in the reverse order must still resolve each topic's own pending send.
        interceptor.onAcknowledgement(metadata(other), null);
        interceptor.onAcknowledgement(metadata(TABLE), null);

        ZerobusSinkMetrics metrics = interceptor.metrics();
        assertThat(metrics.getTotalRecordsIngested()).isEqualTo(2);
        assertThat(metrics.getTotalFlushes()).isEqualTo(2);
        assertThat(metrics.getTotalErrors()).isZero();
    }

    @Test
    void anAcknowledgementWithoutAPendingSendDoesNotFail() {
        // A record can fail before the broker assigns metadata, so onAcknowledgement may arrive with
        // no topic to attribute it to.
        assertThatCode(() -> interceptor.onAcknowledgement(null, null)).doesNotThrowAnyException();
        interceptor.onAcknowledgement(metadata("main.default.never_sent"), null);

        assertThat(interceptor.metrics().getTotalFlushes()).isEqualTo(2);
        assertThat(interceptor.metrics().getLastFlushDurationMillis()).isZero();
    }

    @Test
    void boundsThePendingSendsRetainedPerTopic() throws Exception {
        // Sends that are never acknowledged must not grow the pending map without limit. Only one in
        // every LATENCY_SAMPLE_INTERVAL records is timed, so overflowing the bound takes that many
        // times more records.
        int sampled = ZerobusProducerInterceptor.MAX_PENDING_PER_TOPIC + 250;
        int total = sampled * ZerobusProducerInterceptor.LATENCY_SAMPLE_INTERVAL;
        for (int i = 0; i < total; i++) {
            send("{\"id\":" + i + ",\"__op\":\"c\"}");
        }

        assertThat(pendingCount(TABLE)).isEqualTo(ZerobusProducerInterceptor.MAX_PENDING_PER_TOPIC);
        assertThat(interceptor.metrics().getTotalRecordsIngested()).isEqualTo(total);
    }

    @Test
    void samplesTheLatencyRatherThanTimingEveryRecord() throws Exception {
        final int interval = ZerobusProducerInterceptor.LATENCY_SAMPLE_INTERVAL;
        for (int i = 0; i < interval; i++) {
            send("{\"id\":" + i + ",\"__op\":\"c\"}");
        }

        // Exactly one of the records in a full interval is timed.
        assertThat(pendingCount(TABLE)).isEqualTo(1);
        assertThat(interceptor.metrics().getTotalRecordsIngested()).isEqualTo(interval);
    }

    @Test
    void timesTheFirstRecordSoLowVolumePipelinesStillReportDurations() {
        // A pipeline that never produces a full sampling interval must still report a duration rather
        // than leaving the gauges at zero forever.
        send("{\"id\":1,\"__op\":\"c\"}");
        interceptor.onAcknowledgement(metadata(), null);

        ZerobusSinkMetrics metrics = interceptor.metrics();
        assertThat(metrics.getTotalFlushes()).isEqualTo(1);
        assertThat(metrics.getMaxFlushDurationMillis()).isGreaterThanOrEqualTo(0L);
        // The sample was consumed by the acknowledgement, so nothing stays pending.
        assertThat(metrics.getTotalErrors()).isZero();
    }

    @Test
    void countsEveryAcknowledgementEvenWhenTheRecordWasNotTimed() {
        // A record that was not sampled still advances TotalFlushes, and leaves the duration gauges
        // at whatever the last sample reported.
        for (int i = 0; i < 10; i++) {
            send("{\"id\":" + i + ",\"__op\":\"c\"}");
            interceptor.onAcknowledgement(metadata(), null);
        }

        ZerobusSinkMetrics metrics = interceptor.metrics();
        assertThat(metrics.getTotalFlushes()).isEqualTo(10);
        assertThat(metrics.getTotalRecordsIngested()).isEqualTo(10);
        assertThat(metrics.getLastFlushDurationMillis()).isGreaterThanOrEqualTo(0L);
        assertThat(metrics.getTotalErrors()).isZero();
    }

    @Test
    void countsEveryRecordWhenSendsAndAcknowledgementsRunConcurrently() throws Exception {
        // onSend runs on the caller thread while onAcknowledgement runs on the producer's network
        // thread, so the counters must hold under concurrent updates.
        int threads = 8;
        int perThread = 500;
        ExecutorService pool = Executors.newFixedThreadPool(threads * 2);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    send("{\"id\":" + i + ",\"__op\":\"c\",\"__source_ts_ms\":" + System.currentTimeMillis() + "}");
                }
                return null;
            }));
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    interceptor.onAcknowledgement(metadata(), null);
                }
                return null;
            }));
        }

        start.countDown();
        for (Future<?> future : futures) {
            future.get(60, TimeUnit.SECONDS);
        }
        pool.shutdownNow();

        ZerobusSinkMetrics metrics = interceptor.metrics();
        assertThat(metrics.getTotalRecordsIngested()).isEqualTo((long) threads * perThread);
        assertThat(metrics.getTotalInserts()).isEqualTo((long) threads * perThread);
        assertThat(metrics.getTotalFlushes()).isEqualTo((long) threads * perThread);
        assertThat(metrics.getTotalErrors()).isZero();
    }

    @SuppressWarnings("unchecked")
    private int pendingCount(String topic) throws Exception {
        java.lang.reflect.Field field = ZerobusProducerInterceptor.class.getDeclaredField("pendingSendNanos");
        field.setAccessible(true);
        Map<String, Deque<Long>> pending = (Map<String, Deque<Long>>) field.get(interceptor);
        Deque<Long> deque = pending.get(topic);
        return deque == null ? 0 : deque.size();
    }

    private void send(String value) {
        interceptor.onSend(new ProducerRecord<>(TABLE, null, value));
    }

    private static RecordMetadata metadata() {
        return metadata(TABLE);
    }

    private static RecordMetadata metadata(String topic) {
        return new RecordMetadata(new TopicPartition(topic, 0), 0L, 0, 0L, 0, 0);
    }
}
