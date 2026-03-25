/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.sns;

import io.debezium.server.Images;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.CreateTopicResponse;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages the lifecycle of SNS + SQS test resources via LocalStack container.
 * Creates an SNS topic and subscribes an SQS queue to it for verification.
 */
public class SnsTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse(Images.LOCALSTACK_IMAGE)
            .asCompatibleSubstituteFor("localstack/localstack");

    private static final LocalStackContainer container = new LocalStackContainer(DEFAULT_IMAGE_NAME)
            .withServices(Service.SNS, Service.SQS)
            .withEnv("LOCALSTACK_AUTH_TOKEN", System.getenv("LOCALSTACK_AUTH_TOKEN"));

    private static final AtomicBoolean running = new AtomicBoolean(false);

    private static String topicArn;
    private static String queueUrl;

    // The Debezium outbox destination will resolve to this SNS topic name
    static final String TOPIC_NAME = "testc-inventory-customers";

    private static synchronized void init() throws IOException, InterruptedException {
        if (!running.get()) {
            container.start();
            running.set(true);
        }
    }

    @Override
    public Map<String, String> start() {
        try {
            init();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Create SNS topic
        SnsClient snsClient = snsClient();
        CreateTopicResponse topicResponse = snsClient.createTopic(
                CreateTopicRequest.builder().name(TOPIC_NAME).build());
        topicArn = topicResponse.topicArn();

        // Create SQS queue to subscribe to the SNS topic (for test verification)
        SqsClient sqsClient = sqsClient();
        CreateQueueResponse queueResponse = sqsClient.createQueue(
                CreateQueueRequest.builder().queueName("sns-test-verification-queue").build());
        queueUrl = queueResponse.queueUrl();

        // Get queue ARN for subscription
        String queueArn = sqsClient.getQueueAttributes(
                        GetQueueAttributesRequest.builder()
                                .queueUrl(queueUrl)
                                .attributeNames(QueueAttributeName.QUEUE_ARN)
                                .build())
                .attributes().get(QueueAttributeName.QUEUE_ARN);

        // Subscribe SQS to SNS
        snsClient.subscribe(SubscribeRequest.builder()
                .topicArn(topicArn)
                .protocol("sqs")
                .endpoint(queueArn)
                .build());

        Map<String, String> params = new ConcurrentHashMap<>();
        params.put("debezium.sink.type", "sns");
        params.put("debezium.sink.sns.endpoint", getAWSEndpoint().toString());
        params.put("debezium.sink.sns.region", getAWSRegion());
        params.put("debezium.sink.sns.topic.arn", topicArn);

        return params;
    }

    @Override
    public void stop() {
        try {
            if (container != null) {
                container.stop();
            }
        } catch (Exception e) {
            // ignored
        }
    }

    public static SnsClient snsClient() {
        return SnsClient.builder()
                .endpointOverride(getAWSEndpoint())
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(getAWSAccessKey(), getAWSSecretKey())))
                .region(Region.of(getAWSRegion()))
                .build();
    }

    public static SqsClient sqsClient() {
        return SqsClient.builder()
                .endpointOverride(getAWSEndpoint())
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(getAWSAccessKey(), getAWSSecretKey())))
                .region(Region.of(getAWSRegion()))
                .build();
    }

    public static String getTopicArn() {
        return topicArn;
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
