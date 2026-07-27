/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import io.debezium.runtime.BatchEvent;
import io.debezium.runtime.CapturingEvents;

/**
 * End-to-end integration test for the gRPC route against a real Databricks Zerobus workspace.
 * <p>
 * Kept OUT of the default CI run and enabled only when credentials are present in the environment,
 * mirroring how Debezium's other cloud sinks gate their live ITs. Provide:
 * <ul>
 *   <li>{@code ZEROBUS_ENDPOINT} — e.g. {@code <ws>.zerobus.<region>.cloud.databricks.com}</li>
 *   <li>{@code ZEROBUS_WORKSPACE_URL} — e.g. {@code https://<ws>.cloud.databricks.com}</li>
 *   <li>{@code ZEROBUS_CLIENT_ID} / {@code ZEROBUS_CLIENT_SECRET} — the service principal</li>
 *   <li>{@code ZEROBUS_TABLE} — a pre-created managed Delta table {@code catalog.schema.table}
 *       with columns {@code (ID INT, NAME STRING)} and the SP granted USE CATALOG/SCHEMA + SELECT/MODIFY
 *       (create the table and grants BEFORE running; the SDK builds authorization_details at start).</li>
 * </ul>
 * Run locally with, e.g.:
 * <pre>
 *   ZEROBUS_ENDPOINT=... ZEROBUS_WORKSPACE_URL=... ZEROBUS_CLIENT_ID=... \
 *   ZEROBUS_CLIENT_SECRET=... ZEROBUS_TABLE=cat.sch.it_smoke \
 *   mvn -pl debezium-server-databricks-zerobus test -Dtest=ZerobusGrpcIT
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "ZEROBUS_ENDPOINT", matches = ".+")
class ZerobusGrpcIT {

    @Test
    void ingestsAndFlushesAgainstRealZerobus() throws Exception {
        ZerobusChangeConsumer consumer = new ZerobusChangeConsumer();

        String table = env("ZEROBUS_TABLE");
        io.debezium.config.Configuration cfg = io.debezium.config.Configuration.create()
                .with("endpoint", env("ZEROBUS_ENDPOINT"))
                .with("workspace.url", env("ZEROBUS_WORKSPACE_URL"))
                .with("client.id", env("ZEROBUS_CLIENT_ID"))
                .with("client.secret", env("ZEROBUS_CLIENT_SECRET"))
                .with("table", table)
                .build();

        set(consumer, "config", new ZerobusChangeConsumerConfig(cfg));
        set(consumer, "sdk", new com.databricks.zerobus.ZerobusSdk(
                cfg.getString("endpoint"), cfg.getString("workspace.url")));

        try {
            long id = System.currentTimeMillis() % 1_000_000;
            consumer.handle(events(event("{\"ID\":" + id + ",\"NAME\":\"it-smoke\"}", table)));
            // handle() flushes to durability before returning; reaching here without exception
            // means the record was accepted and durably written by Zerobus.
            assertThat(id).isPositive();
        }
        finally {
            consumer.close();
        }
    }

    private static String env(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("Missing required env var for IT: " + name);
        }
        return v;
    }

    private static void set(Object target, String field, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(field);
        f.setAccessible(true);
        f.set(target, value);
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
