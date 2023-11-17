/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.sqs;

import static io.debezium.server.sqs.SqsTestResourceLifecycleManager.getQueueUrl;
import static io.debezium.server.sqs.SqsTestResourceLifecycleManager.sqsClient;

import jakarta.enterprise.event.Observes;

import org.junit.jupiter.api.Test;

import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

/**
 * Integration test that verifies basic reading from PostgreSQL database and sending to SQS.
 *
 * @author V K
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(SqsTestResourceLifecycleManager.class)
public class SqsIT {

    private static final int MESSAGE_COUNT = 4;

    {
        Testing.Files.delete(SqsTestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(SqsTestConfigSource.OFFSET_STORE_PATH);
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @Test
    public void testSqs() throws Exception {
        Testing.Print.enable();

        final java.util.List<Message> messages = new java.util.ArrayList<>();
        org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(SqsTestConfigSource.waitForSeconds())).until(() -> {
            final ReceiveMessageResponse receiveMessageResponse = sqsClient()
                    .receiveMessage(ReceiveMessageRequest.builder().queueUrl(getQueueUrl()).waitTimeSeconds(3).maxNumberOfMessages(MESSAGE_COUNT).build());
            messages.addAll(receiveMessageResponse.messages());

            return messages.size() >= MESSAGE_COUNT;
        });
    }

}
