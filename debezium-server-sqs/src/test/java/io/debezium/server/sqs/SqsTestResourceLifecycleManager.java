/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.sqs;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.utility.DockerImageName;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * Manages the lifecycle of a SQS test resource via localstack container.
 *
 * @author V K
 */
public class SqsTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("localstack/localstack");

    private static final LocalStackContainer container = new LocalStackContainer(DEFAULT_IMAGE_NAME)
            .withServices(Service.SQS);

    private static final AtomicBoolean running = new AtomicBoolean(false);

    private static String queueUrl;

    private static synchronized void init() throws java.io.IOException, InterruptedException {
        if (!running.get()) {
            container.start();
            running.set(true);
        }
    }

    public static SqsClient sqsClient() {
        return SqsClient
                .builder()
                .endpointOverride(getAWSEndpoint())
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(getAWSAccessKey(), getAWSSecretKey())))
                .region(Region.of(getAWSRegion()))
                .build();
    }

    @Override
    public Map<String, String> start() {
        try {
            init();
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        Map<String, String> params = new ConcurrentHashMap<>();
        params.put("debezium.sink.type", "sqs");
        params.put("debezium.sink.sqs.endpoint", getAWSEndpoint().toString());
        params.put("debezium.sink.sqs.region", getAWSRegion());

        CreateQueueResponse createdQueue = creteTestQueue();
        queueUrl = createdQueue.queueUrl();
        params.put("debezium.sink.sqs.queue.url", queueUrl);

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

    private static CreateQueueResponse creteTestQueue() {
        Map<QueueAttributeName, String> queueAttributes = new java.util.HashMap<>();
        queueAttributes.put(QueueAttributeName.FIFO_QUEUE, "true");
        queueAttributes.put(QueueAttributeName.CONTENT_BASED_DEDUPLICATION, "true");

        return sqsClient()
                .createQueue(CreateQueueRequest.builder().queueName("debezium-cdc-events-queue.fifo").attributes(queueAttributes).build());
    }

    public static String getQueueUrl() {
        return queueUrl;
    }

    public static String getAWSAccessKey() {
        return container.getAccessKey();
    }

    public static String getAWSSecretKey() {
        return container.getSecretKey();
    }

    public static String getAWSRegion() {
        return container.getRegion();
    }

    public static URI getAWSEndpoint() {
        return container.getEndpoint();
    }
}
