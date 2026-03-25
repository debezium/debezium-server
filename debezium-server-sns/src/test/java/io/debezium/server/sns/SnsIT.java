/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.sns;

import static io.debezium.server.sns.SnsTestResourceLifecycleManager.getQueueUrl;
import static io.debezium.server.sns.SnsTestResourceLifecycleManager.sqsClient;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

/**
 * Integration test that verifies basic reading from PostgreSQL database and publishing to SNS topic.
 * Verification is done via an SQS queue subscribed to the SNS topic.
 *
 * @author Rafael Rain
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(SnsTestResourceLifecycleManager.class)
public class SnsIT {

    private static final int MESSAGE_COUNT = 4;

    {
        Testing.Files.delete(SnsTestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(SnsTestConfigSource.OFFSET_STORE_PATH);
    }

    @Test
    public void testSns() throws Exception {
        Testing.Print.enable();

        final List<Message> messages = new ArrayList<>();
        Awaitility.await()
                .atMost(java.time.Duration.ofSeconds(SnsTestConfigSource.waitForSeconds()))
                .until(() -> {
                    final ReceiveMessageResponse response = sqsClient()
                            .receiveMessage(ReceiveMessageRequest.builder()
                                    .queueUrl(getQueueUrl())
                                    .waitTimeSeconds(3)
                                    .maxNumberOfMessages(MESSAGE_COUNT)
                                    .build());
                    messages.addAll(response.messages());
                    return messages.size() >= MESSAGE_COUNT;
                });

        assertThat(messages.size()).isEqualTo(MESSAGE_COUNT);
    }
}
