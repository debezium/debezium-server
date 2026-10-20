/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import io.debezium.engine.Header;
import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;

public class HttpChangeConsumerOauth2Test {

    @Test
    public void documentedNamesSendClientSecretPostAndAudience() throws Exception {
        TokenRequest request = send(Map.of(
                "debezium.sink.http.authentication.oauth2.token_url", "/token",
                "debezium.sink.http.authentication.oauth2.client_id", "my-client",
                "debezium.sink.http.authentication.oauth2.client_secret", "my-secret",
                "debezium.sink.http.authentication.oauth2.client_auth_method", "client_secret_post",
                "debezium.sink.http.authentication.oauth2.params.audience", "https://api.example.com"));
        assertEquals("POST", request.method());
        assertNotNull(request.body());
        assertTrue(request.body().contains("client_id=my-client"), request.body());
        assertTrue(request.body().contains("client_secret=my-secret"), request.body());
        assertTrue(request.body().contains("audience=https%3A%2F%2Fapi.example.com"), request.body());
    }

    @Test
    public void tokenHttpMethodGetChangesTheMethod() throws Exception {
        TokenRequest request = send(Map.of(
                "debezium.sink.http.authentication.oauth2.token_url", "/token",
                "debezium.sink.http.authentication.oauth2.client_id", "my-client",
                "debezium.sink.http.authentication.oauth2.client_secret", "my-secret",
                "debezium.sink.http.authentication.oauth2.token_url.http_method", "GET"));
        assertEquals("GET", request.method());
    }

    @Test
    public void hyphenatedNamesStillBuild() throws Exception {
        TokenRequest request = send(Map.of(
                "debezium.sink.http.authentication.oauth2.token-url", "/token",
                "debezium.sink.http.authentication.oauth2.client-id", "my-client",
                "debezium.sink.http.authentication.oauth2.client-secret", "my-secret"));
        assertEquals("POST", request.method());
    }

    private static TokenRequest send(Map<String, String> oauth) throws Exception {
        AtomicReference<String> tokenMethod = new AtomicReference<>();
        AtomicReference<String> tokenBody = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/token", exchange -> {
            tokenMethod.set(exchange.getRequestMethod());
            tokenBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] response = "{\"access_token\":\"test-token\",\"expires_in\":3600}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.createContext("/events", exchange -> {
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
        int port = server.getAddress().getPort();
        try {
            Map<String, String> values = new LinkedHashMap<>();
            values.put("debezium.sink.http.url", "http://127.0.0.1:" + port + "/events");
            values.put("debezium.sink.http.retries", "1");
            values.put("debezium.sink.http.retry.interval.ms", "1");
            values.put("debezium.format.value", "json");
            values.put("debezium.sink.http.authentication.type", "oauth2");
            for (Map.Entry<String, String> entry : oauth.entrySet()) {
                String value = entry.getValue();
                if (entry.getKey().endsWith("token_url") || entry.getKey().endsWith("token-url")) {
                    value = "http://127.0.0.1:" + port + value;
                }
                values.put(entry.getKey(), value);
            }
            Config config = new SmallRyeConfigBuilder()
                    .withSources(new PropertiesConfigSource(values, "test.properties"))
                    .build();
            HttpChangeConsumer consumer = new HttpChangeConsumer();
            consumer.initWithConfig(config);
            try {
                consumer.handle(capturingEvents(event("{\"id\":1}")));
            }
            finally {
                consumer.close();
            }
            return new TokenRequest(tokenMethod.get(), tokenBody.get());
        }
        finally {
            server.stop(0);
        }
    }

    private record TokenRequest(String method, String body) {
    }

    private static BatchEvent event(String value) {
        return new BatchEvent() {
            @Override
            public Object key() {
                return "key";
            }

            @Override
            public Object value() {
                return value;
            }

            @Override
            public Integer partition() {
                return null;
            }

            @Override
            public org.apache.kafka.connect.source.SourceRecord record() {
                return null;
            }

            @Override
            public String destination() {
                return "dest";
            }

            @Override
            public void commit() {
            }

            @Override
            public <H> List<Header<H>> headers() {
                return List.of();
            }
        };
    }

    private static CapturingEvents<BatchEvent> capturingEvents(BatchEvent event) {
        return new CapturingEvents<>() {
            @Override
            public List<BatchEvent> records() {
                return List.of(event);
            }

            @Override
            public String destination() {
                return "dest";
            }

            @Override
            public String source() {
                return "";
            }

            @Override
            public String engine() {
                return "default";
            }
        };
    }
}
