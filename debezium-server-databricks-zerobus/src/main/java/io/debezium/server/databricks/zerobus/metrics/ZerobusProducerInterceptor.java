/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus.metrics;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;

/**
 * Exposes {@link ZerobusSinkMetrics} for the Kafka delivery route, in which the Zerobus
 * Kafka-compatible endpoint is written to through the generic {@code kafka} sink rather than through
 * the {@code zerobus} or {@code zerobusrest} consumers.
 * <p>
 * Because that route never enters {@code ZerobusChangeConsumer}, the sink cannot instrument itself.
 * A {@link ProducerInterceptor} is the extension point the Kafka producer already provides, so no
 * change to the {@code kafka} sink is required. Register it by setting:
 *
 * <pre>
 * debezium.sink.kafka.producer.interceptor.classes=io.debezium.server.databricks.zerobus.metrics.ZerobusProducerInterceptor
 * </pre>
 *
 * The metrics then appear under
 * {@code debezium.zerobus:type=connector-metrics,context=sink,server=kafka,task=0}, alongside the
 * {@code grpc} and {@code rest} routes, so the same JMX tooling and dashboards apply to all three.
 * <p>
 * Because {@code onSend} runs for every produced record, the metadata extraction is kept off the
 * allocation path: the operation and the source timestamp are read from the record headers when the
 * {@code ExtractNewRecordState} SMT is configured with {@code add.headers=op,source.ts_ms}, and
 * otherwise from the serialized value through {@link MetadataScan}, a single-pass scan that builds no
 * document tree. Zerobus does not persist headers, so using them costs nothing in the target table.
 * <p>
 * Two metrics differ in meaning on this route, because a Kafka producer exposes less than the
 * Zerobus SDK does:
 * <ul>
 * <li>{@code TotalFlushes} counts broker-acknowledged records. On this route the acknowledgement is
 * the durability boundary, which is what a flush represents on the other two routes.</li>
 * <li>{@code TotalRecordsSkipped} stays {@code 0}. The generic sink forwards every record it
 * receives, so there is no skip decision for this interceptor to observe.</li>
 * </ul>
 */
@ThreadSafe
public class ZerobusProducerInterceptor implements ProducerInterceptor<Object, Object> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZerobusProducerInterceptor.class);

    static final String ROUTE = "kafka";

    /**
     * Producer-level configuration option, set as
     * {@code debezium.sink.kafka.producer.zerobus.metrics.log.interval}, that mirrors
     * {@code debezium.sink.zerobus[rest].metrics.log.interval} on the other two routes. It counts
     * records rather than batches, because a producer interceptor observes individual sends.
     */
    public static final String METRICS_LOG_INTERVAL_CONFIG = "zerobus.metrics.log.interval";

    /**
     * Upper bound on the pending send timestamps retained per topic. Every sampled {@code onSend} is
     * followed by at most one matching {@code onAcknowledgement}, so in a healthy producer the deques
     * stay as small as the number of sampled in-flight records. The bound only guards against
     * unbounded growth if an acknowledgement arrives without a topic to attribute it to, which happens
     * when a record fails before the broker assigns metadata.
     */
    static final int MAX_PENDING_PER_TOPIC = 10_000;

    /**
     * Only one in every {@code LATENCY_SAMPLE_INTERVAL} records is timed.
     * <p>
     * {@code onSend} and {@code onAcknowledgement} run on different threads, and a
     * {@code System.nanoTime()} call is an order of magnitude more expensive when a second thread
     * calls it concurrently — measurably more than the counters and the pending-queue lock combined.
     * Sampling therefore dominates the per-record cost of this interceptor, and the duration gauges
     * describe a representative sample rather than every record, while the counters stay exact.
     * <p>
     * Must be a power of two, so that the sampling test is a bit mask.
     */
    static final int LATENCY_SAMPLE_INTERVAL = 128;

    private static final int LATENCY_SAMPLE_MASK = LATENCY_SAMPLE_INTERVAL - 1;

    /** Header names that carry the operation, in the order they are looked up. */
    private static final String[] OPERATION_HEADERS = { "__op", "op" };

    private static final String SOURCE_TS_MS_HEADER = "__source_ts_ms";

    /**
     * Interned single-character operations, so that reading {@code "c"}/{@code "u"}/{@code "d"}/
     * {@code "r"} out of a record allocates no String on the hot path.
     */
    private static final Map<Character, String> OPERATIONS = Map.of(
            'c', "c", 'u', "u", 'd', "d", 'r', "r", 't', "t");

    /**
     * One scanner per thread. {@code onSend} is called on whichever thread produces the record, and the
     * scanner holds only per-record scratch state, so reusing it keeps the path allocation-free without
     * synchronization.
     */
    private static final ThreadLocal<MetadataScan> SCAN = ThreadLocal.withInitial(MetadataScan::new);

    private final Map<String, Deque<Long>> pendingSendNanos = new ConcurrentHashMap<>();

    private volatile ZerobusSinkMetrics metrics;
    private volatile boolean registered;
    private volatile int metricsLogInterval;
    private final AtomicLong recordsSinceLog = new AtomicLong();
    private final AtomicLong sendsSeen = new AtomicLong();

    @Override
    public void configure(Map<String, ?> configs) {
        if (registered) {
            // The producer calls configure() once, but an interceptor instance that is configured
            // again must not leak a second registration of the same object name.
            LOGGER.debug("Zerobus sink metrics are already registered for the '{}' route", ROUTE);
            return;
        }
        metrics = new ZerobusSinkMetrics(ROUTE);
        metrics.register();
        // A producer interceptor observes records rather than connection state, so the connection is
        // reported as usable for as long as this interceptor is reporting at all.
        metrics.setConnected(true);
        metricsLogInterval = parseMetricsLogInterval(configs.get(METRICS_LOG_INTERVAL_CONFIG));
        registered = true;
        LOGGER.info("Zerobus sink metrics registered for the '{}' route", ROUTE);
    }

    private static int parseMetricsLogInterval(Object configured) {
        if (configured == null) {
            return 0;
        }
        try {
            int interval = Integer.parseInt(configured.toString().trim());
            return Math.max(0, interval);
        }
        catch (NumberFormatException e) {
            LOGGER.warn("Ignoring non-numeric value '{}' for '{}'; periodic metric logging stays disabled",
                    configured, METRICS_LOG_INTERVAL_CONFIG);
            return 0;
        }
    }

    @Override
    public ProducerRecord<Object, Object> onSend(ProducerRecord<Object, Object> record) {
        // This runs on every produced record, so it must stay cheap enough to be invisible next to
        // the network cost of the send itself. Record headers are read first because they need no
        // parsing at all; only when they are absent is the serialized value scanned.
        String op = null;
        long sourceTsMs = -1L;
        final Headers headers = record.headers();
        if (headers != null) {
            op = headerOperation(headers);
            sourceTsMs = headerSourceTsMs(headers);
        }
        if (op == null || sourceTsMs < 0) {
            final MetadataScan scan = SCAN.get();
            scan.of(record.value());
            if (op == null) {
                op = scan.operation();
            }
            if (sourceTsMs < 0) {
                sourceTsMs = scan.sourceTsMs();
            }
        }

        metrics.recordIngested(op, sourceTsMs);
        maybeRecordSendNanos(record.topic());
        maybeLogMetrics();
        return record;
    }

    private void maybeLogMetrics() {
        final int interval = metricsLogInterval;
        if (interval > 0 && recordsSinceLog.incrementAndGet() >= interval) {
            recordsSinceLog.set(0);
            metrics.logMetricsSummary();
        }
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            metrics.recordError();
            return;
        }
        // A record is durable once the broker acknowledges it, which is this route's flush boundary.
        // Only a sampled record carries a duration; the others just advance the counter.
        final long latencyMillis = takeLatencyMillis(metadata == null ? null : metadata.topic());
        if (latencyMillis < 0) {
            metrics.flushed();
        }
        else {
            metrics.flushed(latencyMillis);
        }
    }

    @Override
    public void close() {
        if (registered) {
            metrics.setConnected(false);
            metrics.unregister();
            registered = false;
        }
        pendingSendNanos.clear();
        recordsSinceLog.set(0);
    }

    /** Exposed for tests, which assert against the counters this interceptor feeds. */
    ZerobusSinkMetrics metrics() {
        return metrics;
    }

    // -- header metadata ----------------------------------------------------------------------------

    /**
     * Reads the operation from the {@code __op} record header, which the {@code ExtractNewRecordState}
     * SMT adds when it is configured with {@code add.headers=op}. Zerobus does not persist headers, so
     * carrying the metadata there costs nothing in the target table and avoids scanning the value.
     */
    private static String headerOperation(Headers headers) {
        for (String name : OPERATION_HEADERS) {
            final Header header = headers.lastHeader(name);
            if (header == null || header.value() == null) {
                continue;
            }
            final byte[] value = header.value();
            // Header values pass through the configured header converter, so a string arrives JSON
            // encoded, that is quoted. Anything more elaborate, such as a schema envelope, is left to
            // the value scan rather than parsed here.
            final int start = scalarStart(value);
            final int end = scalarEnd(value);
            if (end - start == 1) {
                final String operation = OPERATIONS.get((char) value[start]);
                if (operation != null) {
                    return operation;
                }
            }
            if (end > start && value[start] != '{' && value[start] != '[') {
                return new String(value, start, end - start, StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    /** Reads {@code source.ts_ms} from the {@code __source_ts_ms} record header. */
    private static long headerSourceTsMs(Headers headers) {
        final Header header = headers.lastHeader(SOURCE_TS_MS_HEADER);
        if (header == null || header.value() == null) {
            return -1L;
        }
        final byte[] value = header.value();
        final int end = scalarEnd(value);
        long parsed = 0;
        boolean anyDigit = false;
        for (int i = scalarStart(value); i < end; i++) {
            final byte b = value[i];
            if (b < '0' || b > '9') {
                return -1L;
            }
            parsed = parsed * 10 + (b - '0');
            anyDigit = true;
        }
        return anyDigit ? parsed : -1L;
    }

    /** Index of the first byte of a header scalar, skipping an opening JSON quote. */
    private static int scalarStart(byte[] value) {
        return value.length > 1 && value[0] == '"' ? 1 : 0;
    }

    /** Index just past the last byte of a header scalar, skipping a closing JSON quote. */
    private static int scalarEnd(byte[] value) {
        return value.length > 1 && value[0] == '"' && value[value.length - 1] == '"'
                ? value.length - 1
                : value.length;
    }

    // -- value scanning -----------------------------------------------------------------------------

    /**
     * Extracts the operation and the source timestamp from a serialized change event in a single pass,
     * without building a document tree.
     * <p>
     * Reading the two fields does not justify materializing the whole record: on a representative
     * unwrapped row this is several times cheaper than {@code ObjectMapper.readTree} and allocates
     * nothing per record. Instances are reused per thread, so the scan itself is allocation-free.
     * <p>
     * The scan tracks nesting depth and skips over string contents, so it matches only
     * {@code op}/{@code __op} and {@code __source_ts_ms} as top-level keys, and {@code ts_ms} only
     * inside a top-level {@code source} object. That mirrors
     * {@code ZerobusChangeConsumer.operationOf}/{@code sourceTsMsOf}, which read the same two shapes
     * from the Connect {@code Struct}: the full Debezium envelope, and an unwrapped record produced by
     * {@code ExtractNewRecordState} with {@code add.fields=op,source.ts_ms}.
     */
    static final class MetadataScan {

        private static final String OP = "op";
        private static final String UNWRAPPED_OP = "__op";
        private static final String UNWRAPPED_SOURCE_TS_MS = "__source_ts_ms";
        private static final String SOURCE = "source";
        private static final String TS_MS = "ts_ms";

        private String operation;
        private long sourceTsMs = -1L;

        String operation() {
            return operation;
        }

        long sourceTsMs() {
            return sourceTsMs;
        }

        /** Scans a record value, which may be a {@code String}, a {@code byte[]}, or neither. */
        void of(Object value) {
            operation = null;
            sourceTsMs = -1L;
            if (value instanceof String) {
                scan((String) value);
            }
            else if (value instanceof byte[]) {
                // The JSON converter emits UTF-8; metadata keys and values are ASCII either way.
                scan(new String((byte[]) value, StandardCharsets.UTF_8));
            }
        }

        private void scan(String json) {
            final int length = json.length();
            int depth = 0;
            int index = 0;
            int sourceDepth = -1;
            boolean inSource = false;

            while (index < length) {
                final char current = json.charAt(index);
                if (current == '"') {
                    final int nameStart = ++index;
                    while (index < length) {
                        final char c = json.charAt(index);
                        if (c == '\\') {
                            index += 2;
                            continue;
                        }
                        if (c == '"') {
                            break;
                        }
                        index++;
                    }
                    final int nameLength = Math.min(index, length) - nameStart;
                    index = Math.min(index + 1, length);

                    // A string is a key only when the next non-space character is a colon.
                    int afterName = index;
                    while (afterName < length && json.charAt(afterName) == ' ') {
                        afterName++;
                    }
                    if (afterName >= length || json.charAt(afterName) != ':') {
                        continue;
                    }
                    int valueStart = afterName + 1;
                    while (valueStart < length && json.charAt(valueStart) == ' ') {
                        valueStart++;
                    }
                    if (valueStart >= length) {
                        return;
                    }

                    if (depth == 1) {
                        if (operation == null
                                && (matches(json, nameStart, nameLength, UNWRAPPED_OP) || matches(json, nameStart, nameLength, OP))) {
                            operation = readOperation(json, valueStart);
                        }
                        else if (sourceTsMs < 0 && matches(json, nameStart, nameLength, UNWRAPPED_SOURCE_TS_MS)) {
                            sourceTsMs = readLong(json, valueStart);
                        }
                        else if (matches(json, nameStart, nameLength, SOURCE)) {
                            inSource = true;
                            sourceDepth = depth;
                        }
                    }
                    else if (inSource && depth == sourceDepth + 1 && sourceTsMs < 0
                            && matches(json, nameStart, nameLength, TS_MS)) {
                        sourceTsMs = readLong(json, valueStart);
                    }
                    index = valueStart;
                    continue;
                }

                if (current == '{' || current == '[') {
                    depth++;
                }
                else if (current == '}' || current == ']') {
                    depth--;
                    if (inSource && depth <= sourceDepth) {
                        inSource = false;
                    }
                }
                index++;
            }
        }

        /** Debezium operations are single characters, so the quoted value is read without a substring. */
        private static String readOperation(String json, int valueStart) {
            if (json.charAt(valueStart) != '"') {
                return null;
            }
            final int start = valueStart + 1;
            final int end = json.indexOf('"', start);
            if (end < 0) {
                return null;
            }
            return end == start + 1 ? OPERATIONS.get(json.charAt(start)) : json.substring(start, end);
        }

        private static long readLong(String json, int valueStart) {
            int index = valueStart;
            boolean negative = json.charAt(index) == '-';
            if (negative) {
                index++;
            }
            long parsed = 0;
            boolean anyDigit = false;
            while (index < json.length()) {
                final char c = json.charAt(index++);
                if (c < '0' || c > '9') {
                    break;
                }
                parsed = parsed * 10 + (c - '0');
                anyDigit = true;
            }
            if (!anyDigit) {
                return -1L;
            }
            return negative ? -parsed : parsed;
        }

        private static boolean matches(String json, int offset, int length, String name) {
            if (length != name.length()) {
                return false;
            }
            for (int i = 0; i < length; i++) {
                if (json.charAt(offset + i) != name.charAt(i)) {
                    return false;
                }
            }
            return true;
        }
    }

    // -- send/acknowledgement latency ---------------------------------------------------------------

    /**
     * Times one in every {@link #LATENCY_SAMPLE_INTERVAL} records. Sampling keeps
     * {@code System.nanoTime()} off the common path, which is what dominates the cost here.
     */
    private void maybeRecordSendNanos(String topic) {
        // Counting from zero samples the very first record, so the duration gauges report a value even
        // on a pipeline that never produces a full sampling interval.
        if ((sendsSeen.getAndIncrement() & LATENCY_SAMPLE_MASK) != 0) {
            return;
        }
        Deque<Long> pending = pendingSendNanos.computeIfAbsent(topic, t -> new ArrayDeque<>());
        synchronized (pending) {
            if (pending.size() >= MAX_PENDING_PER_TOPIC) {
                pending.pollFirst();
            }
            pending.addLast(System.nanoTime());
        }
    }

    /**
     * Returns the time the acknowledged record spent in flight, or {@code -1} when this record was not
     * sampled. Zerobus acknowledges every record for a topic on partition 0, so matching
     * acknowledgements against sampled sends per topic in FIFO order pairs them correctly. If a retry
     * were to reorder batches the pairing would drift, which affects only the accuracy of this
     * duration gauge and no other counter.
     */
    private long takeLatencyMillis(String topic) {
        if (topic == null) {
            return -1L;
        }
        Deque<Long> pending = pendingSendNanos.get(topic);
        if (pending == null) {
            return -1L;
        }
        final Long sentAt;
        synchronized (pending) {
            sentAt = pending.pollFirst();
        }
        // Reading the clock only when a sample is actually pending keeps nanoTime off the common path
        // on this side too.
        return sentAt == null ? -1L : (System.nanoTime() - sentAt) / 1_000_000L;
    }
}
