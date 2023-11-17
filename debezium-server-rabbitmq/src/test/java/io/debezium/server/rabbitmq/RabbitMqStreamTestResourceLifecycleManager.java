/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.rabbitmq;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/**
 * Manages the lifecycle of a RabbitMQ cluster test resource.
 */
public class RabbitMqStreamTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    public static final int PORT = 5552;
    public static RabbitMqContainer container = new RabbitMqContainer();
    private static final AtomicBoolean running = new AtomicBoolean(false);

    private static synchronized void init() throws IOException, InterruptedException {
        if (!running.get()) {

            container.start();
            container.execInContainer("rabbitmq-plugins", "enable", "--all");
            running.set(true);
        }
    }

    @Override
    public Map<String, String> start() {
        try {
            init();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Map<String, String> params = new ConcurrentHashMap<>();
        params.put("debezium.sink.rabbitmqstream.connection.host", container.getHost());
        params.put("debezium.sink.rabbitmqstream.connection.port", String.valueOf(getPort()));
        return params;
    }

    @Override
    public void stop() {
        try {
            if (container != null) {
                container.stop();
            }
        }
        catch (Exception e) {
            // ignored
        }
    }

    public static int getPort() {
        return container.getMappedPort(PORT);
    }
}
