/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.debezium.engine.Header;
import io.debezium.runtime.BatchEvent;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;

class TracingUtilTest {

    private static final String TRACEPARENT = "traceparent";
    private static final String SAMPLED_TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
    private static final String SAMPLED_SPAN_ID = "00f067aa0ba902b7";
    private static final String UNSAMPLED_TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
    private static final String UNSAMPLED_SPAN_ID = "b7ad6b7169203331";

    @BeforeAll
    static void registerPropagator() {
        GlobalOpenTelemetry.resetForTest();
        GlobalOpenTelemetry.set(OpenTelemetry.propagating(ContextPropagators.create(W3CTraceContextPropagator.getInstance())));
    }

    @AfterAll
    static void resetPropagator() {
        GlobalOpenTelemetry.resetForTest();
    }

    @Test
    void shouldExtractPropagatedContextOfEachRecord() {
        final SpanContext sampled = Span.fromContext(TracingUtil.tracingContext(
                event(record(traceparent(SAMPLED_TRACE_ID, SAMPLED_SPAN_ID, "01"))))).getSpanContext();
        final SpanContext unsampled = Span.fromContext(TracingUtil.tracingContext(
                event(record(traceparent(UNSAMPLED_TRACE_ID, UNSAMPLED_SPAN_ID, "00"))))).getSpanContext();

        assertThat(sampled.isRemote()).isTrue();
        assertThat(sampled.getTraceId()).isEqualTo(SAMPLED_TRACE_ID);
        assertThat(sampled.getSpanId()).isEqualTo(SAMPLED_SPAN_ID);
        assertThat(sampled.isSampled()).isTrue();
        assertThat(unsampled.isRemote()).isTrue();
        assertThat(unsampled.getTraceId()).isEqualTo(UNSAMPLED_TRACE_ID);
        assertThat(unsampled.getSpanId()).isEqualTo(UNSAMPLED_SPAN_ID);
        assertThat(unsampled.isSampled()).isFalse();
    }

    @Test
    void shouldUseLastHeaderWhenPropagatedContextIsRepeated() {
        final SourceRecord record = record(traceparent(UNSAMPLED_TRACE_ID, UNSAMPLED_SPAN_ID, "00"));
        record.headers().addString(TRACEPARENT, traceparent(SAMPLED_TRACE_ID, SAMPLED_SPAN_ID, "01"));

        final SpanContext extracted = Span.fromContext(TracingUtil.tracingContext(event(record))).getSpanContext();

        assertThat(extracted.getTraceId()).isEqualTo(SAMPLED_TRACE_ID);
        assertThat(extracted.isSampled()).isTrue();
    }

    @Test
    void shouldKeepCurrentContextWithoutValidPropagatedContext() {
        final Span caller = Span.wrap(SpanContext.create(SAMPLED_TRACE_ID, SAMPLED_SPAN_ID, TraceFlags.getSampled(), TraceState.getDefault()));
        try (Scope ignored = caller.makeCurrent()) {
            final Context current = Context.current();

            assertThat(TracingUtil.tracingContext(event(null))).isSameAs(current);
            assertThat(TracingUtil.tracingContext(event(record(null)))).isSameAs(current);
            assertThat(TracingUtil.tracingContext(event(record("not-a-traceparent")))).isSameAs(current);
        }
    }

    private static String traceparent(String traceId, String spanId, String flags) {
        return "00-" + traceId + "-" + spanId + "-" + flags;
    }

    private static SourceRecord record(String traceparent) {
        final SourceRecord record = new SourceRecord(Map.of(), Map.of(), "topic", null, null);
        if (traceparent != null) {
            record.headers().addString(TRACEPARENT, traceparent);
        }
        return record;
    }

    private static BatchEvent event(SourceRecord record) {
        return new BatchEvent() {
            @Override
            public Object key() {
                return null;
            }

            @Override
            public Object value() {
                return null;
            }

            @Override
            public Integer partition() {
                return null;
            }

            @Override
            public SourceRecord record() {
                return record;
            }

            @Override
            public <H> List<Header<H>> headers() {
                return List.of();
            }

            @Override
            public String destination() {
                return "topic";
            }

            @Override
            public void commit() {
            }
        };
    }
}
