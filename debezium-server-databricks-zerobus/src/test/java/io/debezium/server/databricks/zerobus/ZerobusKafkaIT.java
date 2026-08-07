/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Properties;

import javax.management.ObjectName;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * End-to-end integration test for the Kafka route against a real Databricks Zerobus workspace, using
 * the Zerobus Kafka-compatible endpoint together with
 * {@link ZerobusOAuthBearerLoginCallbackHandler} and
 * {@link io.debezium.server.databricks.zerobus.metrics.ZerobusProducerInterceptor}.
 * <p>
 * Kept OUT of the default CI run and enabled only when credentials are present in the environment,
 * mirroring {@link ZerobusGrpcIT}. Provide:
 * <ul>
 *   <li>{@code ZEROBUS_KAFKA_BOOTSTRAP} — e.g. {@code <ws-id>.zerobus.<region>.cloud.databricks.com:9092}</li>
 *   <li>{@code ZEROBUS_WORKSPACE_URL} — e.g. {@code https://<ws>.cloud.databricks.com}</li>
 *   <li>{@code ZEROBUS_WORKSPACE_ID} — the numeric workspace id</li>
 *   <li>{@code ZEROBUS_CLIENT_ID} / {@code ZEROBUS_CLIENT_SECRET} — the service principal</li>
 *   <li>{@code ZEROBUS_TABLE} — a pre-created managed Delta table {@code catalog.schema.table}
 *       with columns {@code (ID INT, NAME STRING)} and the SP granted USE CATALOG/SCHEMA + SELECT/MODIFY</li>
 * </ul>
 * The Kafka-compatible endpoint must also be enabled for the workspace; otherwise authentication
 * fails with {@code feature "Zerobus Ingest Kafka Endpoint" is not enabled ... Error Code: 4062}.
 * <p>
 * Run locally with, e.g.:
 * <pre>
 *   ZEROBUS_KAFKA_BOOTSTRAP=... ZEROBUS_WORKSPACE_URL=... ZEROBUS_WORKSPACE_ID=... \
 *   ZEROBUS_CLIENT_ID=... ZEROBUS_CLIENT_SECRET=... ZEROBUS_TABLE=cat.sch.it_smoke \
 *   mvn -pl debezium-server-databricks-zerobus test -Dtest=ZerobusKafkaIT
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "ZEROBUS_KAFKA_BOOTSTRAP", matches = ".+")
class ZerobusKafkaIT {

    @Test
    void producesToRealZerobusAndReportsSinkMetrics() throws Exception {
        final String table = env("ZEROBUS_TABLE");
        final Properties props = new Properties();
        props.put("bootstrap.servers", env("ZEROBUS_KAFKA_BOOTSTRAP"));
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "OAUTHBEARER");
        props.put("sasl.login.callback.handler.class", ZerobusOAuthBearerLoginCallbackHandler.class.getName());
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
                + "workspaceUrl=\"" + env("ZEROBUS_WORKSPACE_URL") + "\" "
                + "workspaceId=\"" + env("ZEROBUS_WORKSPACE_ID") + "\" "
                + "clientId=\"" + env("ZEROBUS_CLIENT_ID") + "\" "
                + "clientSecret=\"" + env("ZEROBUS_CLIENT_SECRET") + "\" "
                + "tables=\"" + table + "\" ;");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");
        // Zerobus implements neither the transactional APIs nor compressed batches, and Kafka clients
        // enable idempotence by default, which would fail with UnsupportedVersionException.
        props.put("enable.idempotence", "false");
        props.put("compression.type", "none");
        props.put("interceptor.classes", "io.debezium.server.databricks.zerobus.metrics.ZerobusProducerInterceptor");

        final ObjectName metricsName = new ObjectName(
                "debezium.zerobus:type=connector-metrics,context=sink,server=kafka,task=0");
        final long id = System.currentTimeMillis() % 1_000_000;

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            assertThat(ManagementFactory.getPlatformMBeanServer().isRegistered(metricsName)).isTrue();

            RecordMetadata metadata = producer
                    .send(new ProducerRecord<>(table, null,
                            "{\"ID\":" + id + ",\"NAME\":\"it-smoke\",\"__op\":\"c\",\"__source_ts_ms\":"
                                    + System.currentTimeMillis() + "}"))
                    .get();

            // A returned offset means Zerobus accepted and durably wrote the record; a schema mismatch
            // would instead fail with InvalidRecordException.
            assertThat(metadata.offset()).isNotNegative();
            producer.flush();

            assertMetric(metricsName, "TotalRecordsIngested", 1L);
            assertMetric(metricsName, "TotalInserts", 1L);
            assertMetric(metricsName, "TotalFlushes", 1L);
            assertMetric(metricsName, "TotalErrors", 0L);
            assertThat((Long) ManagementFactory.getPlatformMBeanServer()
                    .getAttribute(metricsName, "MilliSecondsBehindSource")).isNotNegative();
            assertThat((String) ManagementFactory.getPlatformMBeanServer()
                    .getAttribute(metricsName, "Route")).isEqualTo("kafka");
        }

        // Closing the producer closes the interceptor, which must deregister the MBean.
        assertThat(ManagementFactory.getPlatformMBeanServer().isRegistered(metricsName)).isFalse();
    }

    @Test
    void reportsAnErrorWhenTheRecordDoesNotMatchTheTableSchema() throws Exception {
        final String table = env("ZEROBUS_TABLE");
        final Properties props = new Properties();
        props.put("bootstrap.servers", env("ZEROBUS_KAFKA_BOOTSTRAP"));
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "OAUTHBEARER");
        props.put("sasl.login.callback.handler.class", ZerobusOAuthBearerLoginCallbackHandler.class.getName());
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
                + "workspaceUrl=\"" + env("ZEROBUS_WORKSPACE_URL") + "\" "
                + "workspaceId=\"" + env("ZEROBUS_WORKSPACE_ID") + "\" "
                + "clientId=\"" + env("ZEROBUS_CLIENT_ID") + "\" "
                + "clientSecret=\"" + env("ZEROBUS_CLIENT_SECRET") + "\" "
                + "tables=\"" + table + "\" ;");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");
        props.put("enable.idempotence", "false");
        props.put("compression.type", "none");
        props.put("interceptor.classes", "io.debezium.server.databricks.zerobus.metrics.ZerobusProducerInterceptor");
        props.put("delivery.timeout.ms", "30000");
        props.put("request.timeout.ms", "20000");

        final ObjectName metricsName = new ObjectName(
                "debezium.zerobus:type=connector-metrics,context=sink,server=kafka,task=0");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            // A column that the target table does not declare is rejected by the broker, which lets
            // the interceptor observe a failed acknowledgement.
            try {
                producer.send(new ProducerRecord<>(table, null, "{\"NOT_A_COLUMN\":1}"))
                        .get(Duration.ofSeconds(45).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
            }
            catch (Exception expected) {
                // The rejection is the expected outcome; it is asserted through the metrics below.
            }

            assertMetric(metricsName, "TotalErrors", 1L);
            assertMetric(metricsName, "TotalFlushes", 0L);
        }
    }

    private static void assertMetric(ObjectName name, String attribute, long expected) throws Exception {
        assertThat((Long) ManagementFactory.getPlatformMBeanServer().getAttribute(name, attribute))
                .as(attribute)
                .isEqualTo(expected);
    }

    private static String env(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("Missing required env var for IT: " + name);
        }
        return v;
    }
}
