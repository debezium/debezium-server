/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rabbitmq;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * rabbitmq container
 */
public class RabbitMqContainer extends GenericContainer<RabbitMqContainer> {

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("rabbitmq:3.10.19-management");
    public static final int BROKER_PORT = 5672;

    public RabbitMqContainer() {
        super(DEFAULT_IMAGE_NAME);
        withExposedPorts(BROKER_PORT, 15672);
    }
}
