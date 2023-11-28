/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import static org.awaitility.Awaitility.await;
import static org.testcontainers.containers.output.OutputFrame.OutputType.STDOUT;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;

import lombok.NonNull;

public class DebeziumTestContainerWrapper extends GenericContainer<DebeziumTestContainerWrapper> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumTestContainerWrapper.class);

    private String networkAlias;

    public DebeziumTestContainerWrapper(@NonNull String dockerImageName) {
        super(dockerImageName);
    }

    public void pause() {
        LOGGER.info("Pausing container " + getContainerName());
        getDockerClient().pauseContainerCmd(getContainerId()).exec();
    }

    public void unpause() {
        LOGGER.info("Unpausing container " + getContainerName());
        getDockerClient().unpauseContainerCmd(getContainerId()).exec();
    }

    public DebeziumTestContainerWrapper withNetworkAlias(String alias) {
        this.networkAlias = alias;
        return super.withNetworkAliases(alias);
    }

    public String getContainerAddress() {
        return networkAlias;
    }

    public String getStandardOutput() {
        return getLogs(STDOUT);
    }

    public void waitForContainerLog(String log) {
        await()
                .atMost(60, TimeUnit.SECONDS)
                .until(() -> getLogs(OutputFrame.OutputType.STDOUT).contains(log));
    }

    public void waitForStop() {
        await()
                .atMost(60, TimeUnit.SECONDS)
                .until(() -> !isRunning());
    }
}
