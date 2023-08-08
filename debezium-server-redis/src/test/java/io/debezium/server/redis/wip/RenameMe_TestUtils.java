/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis.wip;

import static io.debezium.server.redis.wip.TestConstants.POSTGRES_DATABASE;
import static io.debezium.server.redis.wip.TestConstants.POSTGRES_USER;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;

public class RenameMe_TestUtils {
    public static void waitForContainerLog(GenericContainer<?> container, String expectedLog) {
        await()
                .atMost(20, TimeUnit.SECONDS)
                .until(() -> container.getLogs(OutputFrame.OutputType.STDOUT).contains(expectedLog));
    }

    public static void waitForContainerStop(GenericContainer<?> container) {
        await()
                .atMost(20, TimeUnit.SECONDS)
                .until(() -> !container.isRunning());
    }

    static String getContainerIp(GenericContainer<?> container) {
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

    public static String getRedisContainerAddress(TestContainersResource resource) {
        return String.format("%s:%d", getContainerIp(resource.getContainer()), resource.getPort());
    }

    public static void insertCustomerToPostgres(GenericContainer<?> container, String firstName, String lastName, String email)
            throws IOException, InterruptedException {
        container.execInContainer("psql",
                "-U", POSTGRES_USER,
                "-d", POSTGRES_DATABASE,
                "-c", "INSERT INTO inventory.customers VALUES (default,'" + firstName + "','" + lastName + "','" + email + "')");
    }
}
