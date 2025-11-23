/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.Test;

import io.debezium.openlineage.DebeziumTestTransport;
import io.debezium.openlineage.facets.DebeziumConfigFacet;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.transports.TransportBuilder;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

/**
 * Integration test for verifying that the Kafka sink adapter can stream change events from a PostgreSQL database
 * to a configured Apache Kafka broker.
 *
 * @author Alfusainey Jallow
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(KafkaTestResourceLifecycleManager.class)
@TestProfile(OpenLineageProfile.class)
public class KafkaOpenLineageIT extends KafkaBaseIT {

    @Test
    public void testKafkaWithOpenLineage() {
        DebeziumTestTransport debeziumTestTransport = getDebeziumTestTransport();

        testKafka();

        List<OpenLineage.RunEvent> runningEvents = debeziumTestTransport.getRunEvents().stream()
                .filter(e -> e.getEventType() == OpenLineage.RunEvent.EventType.RUNNING)
                .toList();

        assertThat(runningEvents).isNotEmpty();

        OpenLineage.RunEvent startEvent = runningEvents.getFirst();
        assertThat(startEvent.getJob().getNamespace()).isEqualTo("testc");
        assertThat(startEvent.getJob().getName()).isEqualTo("testc.0");
        assertThat(startEvent.getJob().getFacets().getDocumentation().getDescription()).isEqualTo("This connector does cdc for products");

        assertThat(startEvent.getRun().getFacets().getProcessing_engine().getName()).isEqualTo("Debezium");
        assertThat(startEvent.getRun().getFacets().getProcessing_engine().getVersion()).matches("^\\d+\\.\\d+\\.\\d+.*$");
        assertThat(startEvent.getRun().getFacets().getProcessing_engine().getOpenlineageAdapterVersion()).matches("^\\d+\\.\\d+\\.\\d+$");

        DebeziumConfigFacet debeziumConfigFacet = (DebeziumConfigFacet) startEvent.getRun().getFacets().getAdditionalProperties().get("debezium_config");

        assertThat(debeziumConfigFacet.getConfigs()).contains(
                "connector.class=io.debezium.connector.postgresql.PostgresConnector",
                "schema.history.internal.kafka.producer.ssl.endpoint.identification.algorithm=",
                "key.converter.header.schemas.enable=false",
                "transforms=addheader",
                "schema.include.list=inventory",
                "signal.enabled.channels=source,in-process",
                "header.converter.schemas.enable=false",
                "record.processing.threads=",
                "offset.storage.topic=offset-topic",
                "errors.retry.delay.initial.ms=300",
                "key.converter=org.apache.kafka.connect.json.JsonConverter",
                "openlineage.integration.job.tags=env=prod,team=cdc",
                "database.user=postgres",
                "database.dbname=postgres",
                "offset.storage=org.apache.kafka.connect.storage.KafkaOffsetBackingStore",
                "header.converter.key=json",
                "transforms.addheader.type=org.apache.kafka.connect.transforms.InsertHeader",
                "schema.history.internal.kafka.producer.value.serializer=org.apache.kafka.common.serialization.StringSerializer",
                "internal.task.management.timeout.ms=40000",
                "errors.max.retries=-1",
                "transforms.addheader.value.literal=headerValue",
                "database.password=********",
                "name=kafka",
                "transforms.addheader.header=headerKey",
                "openlineage.integration.job.description=This connector does cdc for products",
                "value.converter.header.schemas.enable=false",
                "openlineage.integration.job.owners=Mario=maintainer,John Doe=Data scientist",
                "record.processing.shutdown.timeout.ms=1000",
                "header.converter.header=json",
                "schema.history.internal.kafka.producer.key.serializer=org.apache.kafka.common.serialization.StringSerializer",
                "record.processing.order=ORDERED",
                "topic.prefix=testc",
                "offset.storage.partitions=1",
                "value.converter=org.apache.kafka.connect.json.JsonConverter",
                "header.converter.header.schemas.enable=false",
                "offset.storage.kafka.producer.value.serializer=org.apache.kafka.common.serialization.StringSerializer",
                "openlineage.integration.enabled=true",
                "header.converter.value=json",
                "offset.storage.kafka.producer.ssl.endpoint.identification.algorithm=",
                "key.converter.value=json",
                "value.converter.header=json",
                "key.converter.header=json",
                "offset.flush.timeout.ms=5000",
                "errors.retry.delay.max.ms=10000",
                "value.converter.value=json",
                "offset.flush.interval.ms=0",
                "record.processing.with.serial.consumer=false",
                "database.hostname=localhost",
                "offset.storage.replication.factor=1",
                "table.include.list=inventory.customers",
                "value.converter.key=json",
                "offset.storage.kafka.producer.key.serializer=org.apache.kafka.common.serialization.StringSerializer",
                "key.converter.key=json")
                .anyMatch(config -> config.startsWith("openlineage.integration.config.file.path=") && config.contains("openlineage.yml"))
                .anyMatch(config -> config.startsWith("offset.storage.file.filename=") && config.contains("file-connector-offsets.txt"));

        Map<String, String> tags = startEvent.getJob().getFacets().getTags().getTags()
                .stream()
                .collect(Collectors.toMap(
                        OpenLineage.TagsJobFacetFields::getKey,
                        OpenLineage.TagsJobFacetFields::getValue));

        assertThat(startEvent.getProducer().toString()).startsWith("https://github.com/debezium/debezium/");
        assertThat(tags).contains(entry("env", "prod"), entry("team", "cdc"));

        Map<String, String> ownership = startEvent.getJob().getFacets().getOwnership().getOwners()
                .stream()
                .collect(Collectors.toMap(
                        OpenLineage.OwnershipJobFacetOwners::getName,
                        OpenLineage.OwnershipJobFacetOwners::getType));

        assertThat(ownership).contains(entry("Mario", "maintainer"), entry("John Doe", "Data scientist"));
    }

    private static DebeziumTestTransport getDebeziumTestTransport() {
        ServiceLoader<TransportBuilder> loader = ServiceLoader.load(TransportBuilder.class);
        Optional<TransportBuilder> optionalBuilder = StreamSupport.stream(loader.spliterator(), false)
                .filter(b -> b.getType().equals("debezium"))
                .findFirst();

        return (DebeziumTestTransport) optionalBuilder.orElseThrow(
                () -> new IllegalArgumentException("Failed to find TransportBuilder")).build(null);
    }
}
