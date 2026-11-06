/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.transforms.tracing.ActivateTracingSpan;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.extension.trace.propagation.B3Propagator;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;

public class PulsarChangeConsumerTracingTest {

    private static final String TOPIC = "inventory.customers";
    private static final String CONTEXT_FIELD = "tracingspancontext";
    private static final String SAMPLED_TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
    private static final String SAMPLED_PARENT_ID = "00f067aa0ba902b7";
    private static final String UNSAMPLED_TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
    private static final String UNSAMPLED_PARENT_ID = "b7ad6b7169203331";

    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct().optional()
            .field("table", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
            .build();
    private static final Schema ROW_SCHEMA = SchemaBuilder.struct().optional()
            .field("id", Schema.INT32_SCHEMA)
            .field(CONTEXT_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    private static final Envelope ENVELOPE = Envelope.defineSchema()
            .withName("inventory.customers.Envelope")
            .withRecord(ROW_SCHEMA)
            .withSource(SOURCE_SCHEMA)
            .build();

    private static final InMemorySpanExporter EXPORTER = InMemorySpanExporter.create();
    private static OpenTelemetrySdk openTelemetry;

    private final ActivateTracingSpan<SourceRecord> transform = new ActivateTracingSpan<>();
    private final List<SpanContext> sendContexts = new ArrayList<>();
    private final List<Map<String, String>> sentProperties = new ArrayList<>();
    private final AtomicInteger commits = new AtomicInteger();
    private PulsarChangeConsumer consumer;
    private Producer<Object> producer;

    @BeforeAll
    static void setUpOpenTelemetry() {
        GlobalOpenTelemetry.resetForTest();
        openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(SdkTracerProvider.builder()
                        .setSampler(Sampler.parentBased(Sampler.alwaysOn()))
                        .addSpanProcessor(SimpleSpanProcessor.create(EXPORTER))
                        .build())
                .setPropagators(ContextPropagators.create(B3Propagator.injectingSingleHeader()))
                .buildAndRegisterGlobal();
    }

    @AfterAll
    static void tearDownOpenTelemetry() {
        openTelemetry.close();
        GlobalOpenTelemetry.resetForTest();
    }

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        EXPORTER.reset();
        transform.configure(Map.of(ActivateTracingSpan.TRACING_SPAN_CONTEXT_FIELD.name(), CONTEXT_FIELD));

        producer = mock(Producer.class);
        when(producer.newMessage()).thenAnswer(invocation -> newMessage());
        when(producer.flushAsync()).thenReturn(CompletableFuture.completedFuture(null));

        consumer = new PulsarChangeConsumer();
        setField(consumer, "config", new PulsarChangeConsumerConfig(Configuration.empty()));
        ((Map<String, Producer<?>>) getField(consumer, "producers")).put(TOPIC, producer);
    }

    @Test
    void shouldSendEachRecordWithinItsPropagatedTraceContext() throws Exception {
        final SourceRecord sampled = transform.apply(changeEvent(1, b3Properties(SAMPLED_TRACE_ID, SAMPLED_PARENT_ID, "1")));
        final SourceRecord unsampled = transform.apply(changeEvent(2, b3Properties(UNSAMPLED_TRACE_ID, UNSAMPLED_PARENT_ID, "0")));
        final SpanData sampledLogWrite = logWriteSpan(SAMPLED_TRACE_ID);
        EXPORTER.reset();

        consumer.handle(events(sampled, unsampled));

        assertThat(sendContexts).hasSize(2);
        assertThat(sendContexts.get(0).getTraceId()).isEqualTo(SAMPLED_TRACE_ID);
        assertThat(sendContexts.get(0).isSampled()).isTrue();
        assertThat(sendContexts.get(1).getTraceId()).isEqualTo(UNSAMPLED_TRACE_ID);
        assertThat(sendContexts.get(1).isSampled()).isFalse();

        final List<SpanData> producerSpans = EXPORTER.getFinishedSpanItems();
        assertThat(producerSpans).hasSize(1);
        assertThat(producerSpans.get(0).getKind()).isEqualTo(SpanKind.PRODUCER);
        assertThat(producerSpans.get(0).getTraceId()).isEqualTo(SAMPLED_TRACE_ID);
        assertThat(producerSpans.get(0).getParentSpanId()).isEqualTo(sampledLogWrite.getSpanId());

        assertThat(sentProperties.get(0)).containsEntry("b3", headerValue(sampled, "b3"));
        assertThat(sentProperties.get(1)).containsEntry("b3", headerValue(unsampled, "b3"));
        assertThat(commits).hasValue(2);
        assertThat(Span.current().getSpanContext().isValid()).isFalse();
    }

    @Test
    void shouldKeepCallerContextForRecordsWithoutValidTraceContext() throws Exception {
        final SourceRecord withoutHeaders = changeEvent(1, null);
        final SourceRecord malformed = changeEvent(2, null);
        malformed.headers().addString("b3", "not-a-b3-header");

        final Span caller = GlobalOpenTelemetry.getTracer("test").spanBuilder("caller").startSpan();
        try (Scope ignored = caller.makeCurrent()) {
            consumer.handle(events(withoutHeaders, malformed));
            assertThat(Span.current()).isSameAs(caller);
        }
        finally {
            caller.end();
        }

        assertThat(sendContexts).containsExactly(caller.getSpanContext(), caller.getSpanContext());
        assertThat(commits).hasValue(2);
    }

    @Test
    void shouldRestoreCallerContextWhenSendFails() {
        final SourceRecord sampled = transform.apply(changeEvent(1, b3Properties(SAMPLED_TRACE_ID, SAMPLED_PARENT_ID, "1")));
        when(producer.newMessage()).thenAnswer(invocation -> {
            final TypedMessageBuilder<Object> message = mock(TypedMessageBuilder.class, RETURNS_SELF);
            when(message.sendAsync()).thenAnswer(send -> {
                sendContexts.add(Span.current().getSpanContext());
                throw new IllegalStateException("send failed");
            });
            return message;
        });

        assertThatThrownBy(() -> consumer.handle(events(sampled))).isInstanceOf(IllegalStateException.class);

        assertThat(sendContexts).extracting(SpanContext::getTraceId).containsExactly(SAMPLED_TRACE_ID);
        assertThat(Context.current()).isSameAs(Context.root());
        assertThat(commits).hasValue(0);
    }

    @SuppressWarnings("unchecked")
    private TypedMessageBuilder<Object> newMessage() {
        final TypedMessageBuilder<Object> message = mock(TypedMessageBuilder.class, RETURNS_SELF);
        when(message.properties(any())).thenAnswer(invocation -> {
            sentProperties.add(new HashMap<>(invocation.getArgument(0)));
            return message;
        });
        when(message.sendAsync()).thenAnswer(invocation -> {
            sendContexts.add(Span.current().getSpanContext());
            GlobalOpenTelemetry.getTracer("pulsar-producer-instrumentation")
                    .spanBuilder(TOPIC + " publish")
                    .setSpanKind(SpanKind.PRODUCER)
                    .startSpan()
                    .end();
            return CompletableFuture.completedFuture(MessageId.earliest);
        });
        return message;
    }

    private SpanData logWriteSpan(String traceId) {
        return EXPORTER.getFinishedSpanItems().stream()
                .filter(span -> span.getName().equals("db-log-write") && span.getTraceId().equals(traceId))
                .findFirst()
                .orElseThrow();
    }

    private static String b3Properties(String traceId, String parentId, String sampled) {
        return "b3=" + traceId + "-" + parentId + "-" + sampled + "\n";
    }

    private static SourceRecord changeEvent(int id, String tracingContext) {
        final Struct row = new Struct(ROW_SCHEMA).put("id", id).put(CONTEXT_FIELD, tracingContext);
        final Struct source = new Struct(SOURCE_SCHEMA).put("table", "customers").put("ts_ms", 1588252618953L);
        return new SourceRecord(Map.of(), Map.of(), TOPIC, ENVELOPE.schema(), ENVELOPE.create(row, source, Instant.now()));
    }

    private static String headerValue(SourceRecord record, String key) {
        return record.headers().lastWithName(key).value().toString();
    }

    private CapturingEvents<BatchEvent> events(SourceRecord... records) {
        final List<BatchEvent> batch = new ArrayList<>();
        for (SourceRecord record : records) {
            batch.add(new BatchEvent() {
                @Override
                public Object key() {
                    return null;
                }

                @Override
                public Object value() {
                    return record.value().toString().getBytes();
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
                @SuppressWarnings("unchecked")
                public <H> List<io.debezium.engine.Header<H>> headers() {
                    final List<io.debezium.engine.Header<H>> headers = new ArrayList<>();
                    record.headers().forEach(header -> headers.add(new io.debezium.engine.Header<>() {
                        @Override
                        public String getKey() {
                            return header.key();
                        }

                        @Override
                        public H getValue() {
                            return (H) header.value();
                        }
                    }));
                    return headers;
                }

                @Override
                public String destination() {
                    return TOPIC;
                }

                @Override
                public void commit() {
                    commits.incrementAndGet();
                }
            });
        }
        return new CapturingEvents<>() {
            @Override
            public List<BatchEvent> records() {
                return batch;
            }

            @Override
            public String destination() {
                return TOPIC;
            }

            @Override
            public String source() {
                return "default";
            }

            @Override
            public String engine() {
                return "default";
            }
        };
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        final Field field = PulsarChangeConsumer.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object getField(Object target, String name) throws Exception {
        final Field field = PulsarChangeConsumer.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
