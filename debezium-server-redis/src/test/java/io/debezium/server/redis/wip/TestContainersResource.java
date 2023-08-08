/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis.wip;

import static org.testcontainers.containers.output.OutputFrame.OutputType.STDOUT;

import java.util.List;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import lombok.Getter;

public class TestContainersResource {
    private final String image;
    @Getter
    private final int port;
    private final List<String> env;
    private String waitForLogRegex;

    @Getter
    private GenericContainer<?> container;

    public TestContainersResource(String image, int port, List<String> env) {
        this.image = image;
        this.port = port;
        this.env = env;
    }

    public TestContainersResource(String image, int port, List<String> env, String waitForLogMessage) {
        this.image = image;
        this.port = port;
        this.env = env;
        this.waitForLogRegex = waitForLogMessage;
    }

    public void start() {
        container = new GenericContainer<>(image);
        container.setEnv(env);
        container.setExposedPorts(List.of(port));
        if (waitForLogRegex != null) {
            container.waitingFor(new LogMessageWaitStrategy().withRegEx(waitForLogRegex));
        }
        container.start();

    }

    // TODO is this best solution?
    public boolean isRunning() {
        return container != null && container.isRunning();
    }

    public void stop() {
        container.stop();
    }

    public String getStandardOutput() {
        return container.getLogs(STDOUT);
    }
}
