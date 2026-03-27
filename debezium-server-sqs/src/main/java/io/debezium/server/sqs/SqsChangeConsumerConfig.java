/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.sqs;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link SqsChangeConsumer}.
 */
public class SqsChangeConsumerConfig {

    public static final Field REGION = Field.create("region")
            .withDisplayName("AWS Region")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("AWS region for the SQS queue.");

    public static final Field ENDPOINT = Field.create("endpoint")
            .withDisplayName("SQS Endpoint Override")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Optional SQS endpoint override (for local testing with LocalStack).");

    public static final Field QUEUE_URL = Field.create("queue.url")
            .withDisplayName("SQS Queue URL")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The URL of the SQS queue to send messages to.");

    public static final Field CREDENTIALS_PROFILE = Field.create("credentials.profile")
            .withDisplayName("AWS Credentials Profile")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("AWS credentials profile name to use for authentication.");

    public static final Field FIFO_MESSAGE_GROUP_ID = Field.create("fifo.message.group.id")
            .withDisplayName("FIFO Message Group ID")
            .withType(ConfigDef.Type.STRING)
            .withDefault("cdc-group")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Message group ID for FIFO queues (only used when queue URL ends with .fifo).");

    // Instance fields
    private String region;
    private String endpoint;
    private String queueUrl;
    private String credentialsProfile;
    private String fifoMessageGroupId;

    public SqsChangeConsumerConfig(Configuration config) {
        init(config);
    }

    protected void init(Configuration config) {
        region = config.getString(REGION);
        endpoint = config.getString(ENDPOINT);
        queueUrl = config.getString(QUEUE_URL);
        credentialsProfile = config.getString(CREDENTIALS_PROFILE);
        fifoMessageGroupId = config.getString(FIFO_MESSAGE_GROUP_ID);
    }

    public String getRegion() {
        return region;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getQueueUrl() {
        return queueUrl;
    }

    public String getCredentialsProfile() {
        return credentialsProfile;
    }

    public String getFifoMessageGroupId() {
        return fifoMessageGroupId;
    }
}
