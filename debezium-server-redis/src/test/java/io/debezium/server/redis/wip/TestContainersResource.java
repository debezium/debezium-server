/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis.wip;

import static org.testcontainers.containers.output.OutputFrame.OutputType.STDOUT;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.shaded.org.apache.commons.lang3.NotImplementedException;

import lombok.Getter;
import lombok.NonNull;

/**
 * Manages the containers ran during tests
 *
 */
public class TestContainersResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestContainersResource.class);

    private final String name;
    private final String image;
    @Getter
    private final int port;
    private final String waitForLogRegex;
    private List<String> env;

    @Getter
    private GenericContainer<?> container;

    public TestContainersResource(@NonNull String image, int port, List<String> env, String waitForLogMessage, @NonNull String name) {
        this.image = image;
        this.port = port;
        this.env = env;
        this.waitForLogRegex = waitForLogMessage;
        this.name = name;
    }

    public void start() {
        LOGGER.info("Starting container " + name);
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

    public boolean isRunning() {
        return container != null && container.isRunning();
    }

    public void stop() {
        LOGGER.info("Stopping container " + name);
        container.stop();
    }

    public void pause() {
        LOGGER.info("Pausing container " + image);
        container.getDockerClient().pauseContainerCmd(container.getContainerId()).exec();
    }

    public void resume() {
        LOGGER.info("Resuming container " + name);
        container.getDockerClient().unpauseContainerCmd(container.getContainerId()).exec();
    }

    public String getStandardOutput() {
        return container.getLogs(STDOUT);
    }

    public void setEnv(List<String> env) {
        if (isRunning()) {
            // TODO: consider restarting with different env
            throw new NotImplementedException("cannot edit running container");
        }

        this.env = env;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getContainerIp() {
        return container
                .getContainerInfo()
                .getNetworkSettings()
                .getNetworks()
                .entrySet()
                .stream()
                .findFirst()
                .get()
                .getValue()
                .getIpAddress();
    }

    public static class Builder {
        private String image;
        private int port = 0;
        private List<String> env;
        private String waitForLogRegex;
        private String name;

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

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public TestContainersResource build() {
            return new TestContainersResource(image, port, env, waitForLogRegex, name);
        }
    }
}
