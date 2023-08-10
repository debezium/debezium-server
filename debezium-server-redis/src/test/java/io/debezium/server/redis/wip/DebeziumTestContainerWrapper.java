/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis.wip;

import static org.testcontainers.containers.output.OutputFrame.OutputType.STDOUT;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import lombok.NonNull;

public class DebeziumTestContainerWrapper extends GenericContainer<DebeziumTestContainerWrapper> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumTestContainerWrapper.class);

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

    public String getContainerIp() {
        return getContainerInfo()
                .getNetworkSettings()
                .getNetworks()
                .entrySet()
                .stream()
                .findFirst()
                .get()
                .getValue()
                .getIpAddress();
    }

    public String getStandardOutput() {
        return getLogs(STDOUT);
    }
}
