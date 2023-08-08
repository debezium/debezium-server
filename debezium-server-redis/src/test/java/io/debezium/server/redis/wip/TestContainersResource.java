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
import org.testcontainers.shaded.org.apache.commons.lang3.NotImplementedException;

import lombok.Getter;
import lombok.NonNull;

public class TestContainersResource {
    private final String image;
    @Getter
    private final int port;
    private List<String> env;
    private String waitForLogRegex;

    @Getter
    private GenericContainer<?> container;

    public TestContainersResource(String image, int port, List<String> env) {
        this.image = image;
        this.port = port;
        this.env = env;
    }

    public TestContainersResource(@NonNull String image, int port, List<String> env, String waitForLogMessage) {
        this.image = image;
        this.port = port;
        this.env = env;
        this.waitForLogRegex = waitForLogMessage;
    }

    public void start() {
        container = new GenericContainer<>(image);
        if (env != null) {
            container.setEnv(env);
        }
        if (port > 0) {
            container.setExposedPorts(List.of(port));
        }
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

    public void setEnv(List<String> env) {
        if (isRunning()) {
            throw new NotImplementedException("cannot edit running container.. for now");
        }

        this.env = env;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String image;
        private int port = 0;
        private List<String> env;
        private String waitForLogRegex;

        public Builder withImage(String image) {
            this.image = image;
            return this;
        }

        public Builder withPort(int port) {
            this.port = port;
            return this;
        }

        public Builder withEnv(List<String> env) {
            this.env = env;
            return this;
        }

        public Builder withWaitForRegex(String waitForLogRegex) {
            this.waitForLogRegex = waitForLogRegex;
            return this;
        }

        public TestContainersResource build() {
            return new TestContainersResource(image, port, env, waitForLogRegex);
        }
    }
}
