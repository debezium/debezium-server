/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.debezium.doc.FixFor;
import io.quarkus.test.QuarkusUnitTest;

public class InvalidConnectorStartupIT {

    // System properties (ordinal 400) win over all config sources without going through
    // Quarkus-validated application.properties (which rejects unknown config roots).
    static {
        System.setProperty("debezium.sink.type", "test");
        System.setProperty("debezium.source.connector.class", "invalid.connector.ClassName");
    }

    @RegisterExtension
    static final QuarkusUnitTest config = new QuarkusUnitTest()
            .setArchiveProducer(() -> ShrinkWrap.create(JavaArchive.class)
                    .addClasses(DebeziumServer.class, ChangeConsumerFactory.class,
                            DefaultChangeConsumer.class, ConnectorLifecycle.class,
                            TestConsumer.class, TestConfigSource.class)
                    .addPackages(true, "io.debezium.server.events"))
            .overrideConfigKey("quarkus.arc.remove-unused-beans", "false")
            .overrideConfigKey("quarkus.kubernetes-client.devservices.enabled", "false")
            .assertException(t -> {
                assertThat(t).isNotInstanceOf(StackOverflowError.class);
                assertThat(t).hasRootCauseInstanceOf(ClassNotFoundException.class);
            });

    @AfterAll
    static void cleanup() {
        System.clearProperty("debezium.sink.type");
        System.clearProperty("debezium.source.connector.class");
    }

    @Test
    @FixFor("DBZ-8703")
    @DisplayName("Invalid connector class causes clean startup failure with ClassNotFoundException")
    public void shouldFailCleanlyWithInvalidConnectorClass() {
        fail("Quarkus should have failed to start due to invalid connector class");
    }
}
