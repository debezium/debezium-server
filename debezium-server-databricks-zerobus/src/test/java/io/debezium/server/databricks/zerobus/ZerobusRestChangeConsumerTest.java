/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.debezium.server.databricks.zerobus.metrics.ZerobusSinkMetrics;

/**
 * Verifies the REST route's HTTP contract by mocking the JDK {@link HttpClient} (as suggested for
 * this route in issue #2279): asserts the request path, headers and JSON body, and the error and
 * skip behavior — all without any Zerobus dependency or network.
 */
class ZerobusRestChangeConsumerTest {

    private ZerobusRestChangeConsumer consumer;
    private HttpClient httpClient;

    @BeforeEach
    void setUp() throws Exception {
        consumer = new ZerobusRestChangeConsumer();
        httpClient = mock(HttpClient.class);

        Configuration cfg = Configuration.create()
                .with("uri", "https://zerobus.example.com/")
                .with("workspace.url", "https://dbc-a1b2.cloud.databricks.com")
                .with("workspace.id", "1234567890123456")
                .with("client.id", "sp-client")
                .with("client.secret", "sp-secret")
                .with("table", "main.default.customers")
                .build();

        set(consumer, "config", new ZerobusRestChangeConsumerConfig(cfg));
        set(consumer, "baseUri", "https://zerobus.example.com");
        set(consumer, "httpClient", httpClient);
        // token provider with a pre-seeded (non-expiring) token, so currentToken() never hits the network
        set(consumer, "tokenProvider", tokenProviderWithCachedToken(
                "https://dbc-a1b2.cloud.databricks.com", "1234567890123456", "sp-client", "sp-secret",
                "main.default.customers", "test-token-xyz"));
    }

    @Test
    void postsInsertWithCorrectPathHeadersAndBody() throws Exception {
        stubResponse(200, "{}");
        ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);

        consumer.handle(events(event("{\"id\":1,\"name\":\"a\"}", "oradbz.main.default.customers")));

        // capture the request the sink built
        org.mockito.Mockito.verify(httpClient).send(captor.capture(), any());
        HttpRequest req = captor.getValue();

        assertThat(req.uri().toString())
                .isEqualTo("https://zerobus.example.com/zerobus/v1/tables/main.default.customers/insert");
        assertThat(req.method()).isEqualTo("POST");
        assertThat(req.headers().firstValue("Authorization")).contains("Bearer test-token-xyz");
        assertThat(req.headers().firstValue("Content-Type")).contains("application/json");
        assertThat(req.headers().firstValue("x-databricks-zerobus-table-name")).contains("main.default.customers");
        assertThat(bodyOf(req)).isEqualTo("{\"id\":1,\"name\":\"a\"}");
    }

    @Test
    void skipsTombstoneWithoutPosting() throws Exception {
        consumer.handle(events(event(null, "oradbz.main.default.customers")));
        org.mockito.Mockito.verify(httpClient, org.mockito.Mockito.never()).send(any(), any());
    }

    @Test
    void throwsOnNonSuccessStatus() throws Exception {
        stubResponse(500, "boom");
        assertThatThrownBy(() -> consumer.handle(events(event("{\"id\":1}", "oradbz.main.default.customers"))))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("main.default.customers");
    }

    @Test
    void recordsIngestAndFlushMetricsOnSuccessfulPost() throws Exception {
        stubResponse(200, "{}");
        ZerobusSinkMetrics metrics = mock(ZerobusSinkMetrics.class);
        set(consumer, "metrics", metrics);

        consumer.handle(events(event("{\"id\":1}", "oradbz.main.default.customers")));

        org.mockito.Mockito.verify(metrics).recordIngested(null, -1L);
        org.mockito.Mockito.verify(metrics).flushed(org.mockito.ArgumentMatchers.anyLong());
    }

    @Test
    void recordsErrorMetricOnNonSuccessStatus() throws Exception {
        stubResponse(500, "boom");
        ZerobusSinkMetrics metrics = mock(ZerobusSinkMetrics.class);
        set(consumer, "metrics", metrics);

        assertThatThrownBy(() -> consumer.handle(events(event("{\"id\":1}", "oradbz.main.default.customers"))))
                .isInstanceOf(DebeziumException.class);
        org.mockito.Mockito.verify(metrics).recordError();
    }

    // --- helpers -------------------------------------------------------------

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void stubResponse(int status, String body) throws Exception {
        HttpResponse resp = mock(HttpResponse.class);
        when(resp.statusCode()).thenReturn(status);
        when(resp.body()).thenReturn(body);
        when(httpClient.send(any(), any())).thenReturn(resp);
    }

    private static String bodyOf(HttpRequest req) {
        var bp = req.bodyPublisher().orElseThrow();
        var sub = new java.util.concurrent.Flow.Subscriber<java.nio.ByteBuffer>() {
            final StringBuilder sb = new StringBuilder();

            public void onSubscribe(java.util.concurrent.Flow.Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            public void onNext(java.nio.ByteBuffer b) {
                sb.append(java.nio.charset.StandardCharsets.UTF_8.decode(b));
            }

            public void onError(Throwable t) {
            }

            public void onComplete() {
            }
        };
        bp.subscribe(sub);
        return sub.sb.toString();
    }

    private static void set(Object target, String field, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(field);
        f.setAccessible(true);
        f.set(target, value);
    }

    private static ZerobusTokenProvider tokenProviderWithCachedToken(String wsUrl, String wsId, String clientId,
                                                                     String secret, String tables, String token)
            throws Exception {
        ZerobusTokenProvider tp = new ZerobusTokenProvider(wsUrl, wsId, clientId, secret, tables);
        set(tp, "cachedToken", token);
        Field expiry = ZerobusTokenProvider.class.getDeclaredField("expiryMs");
        expiry.setAccessible(true);
        expiry.setLong(tp, Long.MAX_VALUE);
        return tp;
    }

    private static BatchEvent event(String value, String destination) {
        return new BatchEvent() {
            public Object key() {
                return null;
            }

            public Object value() {
                return value;
            }

            public Integer partition() {
                return 0;
            }

            public org.apache.kafka.connect.source.SourceRecord record() {
                return null;
            }

            public String destination() {
                return destination;
            }

            public void commit() {
            }
        };
    }

    private static CapturingEvents<BatchEvent> events(BatchEvent... evts) {
        List<BatchEvent> list = new ArrayList<>(List.of(evts));
        return new CapturingEvents<>() {
            public List<BatchEvent> records() {
                return list;
            }

            public String destination() {
                return null;
            }

            public String source() {
                return null;
            }

            public String engine() {
                return null;
            }
        };
    }
}
