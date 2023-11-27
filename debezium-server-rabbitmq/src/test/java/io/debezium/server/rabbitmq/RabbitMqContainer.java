/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rabbitmq;

import java.time.Duration;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * RabbitMQ container
 */
public class RabbitMqContainer extends GenericContainer<RabbitMqContainer> {

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("rabbitmq:3.12.9-management");
    public static final int BROKER_PORT = 5672;
    public static final int STREAM_PORT = 5552;

    public RabbitMqContainer() {
        super(DEFAULT_IMAGE_NAME);
        withExposedPorts(BROKER_PORT, STREAM_PORT, 15672);

        this.waitStrategy = Wait.forLogMessage(".*Server startup complete.*", 1).withStartupTimeout(Duration.ofSeconds(60));
    }
}
