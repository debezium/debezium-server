/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.sns;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link SnsChangeConsumer}.
 */
public class SnsChangeConsumerConfig {

    public static final int MAX_BATCH_SIZE = 10;
    public static final int DEFAULT_RETRY_COUNT = 5;
    public static final int MAX_SNS_MESSAGE_BYTES = 256 * 1024;

    public static final Field REGION = Field.create("region")
            .withDisplayName("AWS Region")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("AWS region for the SNS topic.");

    public static final Field ENDPOINT = Field.create("endpoint")
            .withDisplayName("SNS Endpoint Override")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Optional SNS endpoint override (for local testing with LocalStack).");

    public static final Field CREDENTIALS_PROFILE = Field.create("credentials.profile")
            .withDisplayName("AWS Credentials Profile")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("AWS credentials profile name to use for authentication.");

    public static final Field TOPIC_ARN = Field.create("topic.arn")
            .withDisplayName("Default Topic ARN")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Default SNS topic ARN to publish messages to.");

    public static final Field TOPIC_ARN_PREFIX = Field.create("topic.arn.prefix")
            .withDisplayName("Topic ARN Prefix")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Prefix to prepend to the destination name to compose the full topic ARN.");

    public static final Field DEFAULT_RETRIES = Field.create("default.retries")
            .withDisplayName("Default Retries")
            .withType(ConfigDef.Type.INT)
            .withDefault(DEFAULT_RETRY_COUNT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum number of retry attempts for failed requests.");

    public static final Field FIFO_MESSAGE_GROUP_ID_HEADER = Field.create("fifo.message.group.id.header")
            .withDisplayName("FIFO Message Group ID Header")
            .withType(ConfigDef.Type.STRING)
            .withDefault("aggregateId")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Header name to use as the MessageGroupId for FIFO topics.");

    public static final Field FIFO_MESSAGE_DEDUP_ID_HEADER = Field.create("fifo.message.dedup.id.header")
            .withDisplayName("FIFO Message Deduplication ID Header")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Header name to use as the MessageDeduplicationId for FIFO topics.");

    public static final Field FIFO_DEFAULT_GROUP_ID = Field.create("fifo.default.group.id")
            .withDisplayName("FIFO Default Group ID")
            .withType(ConfigDef.Type.STRING)
            .withDefault("default")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Default MessageGroupId when no header value is found for FIFO topics.");

    // Instance fields
    private String region;
    private String endpoint;
    private String credentialsProfile;
    private String topicArn;
    private String topicArnPrefix;
    private int maxRetries;
    private String messageGroupIdHeader;
    private String messageDeduplicationIdHeader;
    private String fifoDefaultGroupId;

    public SnsChangeConsumerConfig(Configuration config) {
        init(config);
    }

    protected void init(Configuration config) {
        region = config.getString(REGION);
        endpoint = config.getString(ENDPOINT);
        credentialsProfile = config.getString(CREDENTIALS_PROFILE);
        topicArn = config.getString(TOPIC_ARN);
        topicArnPrefix = config.getString(TOPIC_ARN_PREFIX);
        maxRetries = config.getInteger(DEFAULT_RETRIES);
        messageGroupIdHeader = config.getString(FIFO_MESSAGE_GROUP_ID_HEADER);
        messageDeduplicationIdHeader = config.getString(FIFO_MESSAGE_DEDUP_ID_HEADER);
        fifoDefaultGroupId = config.getString(FIFO_DEFAULT_GROUP_ID);
    }

    public String getRegion() {
        return region;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getCredentialsProfile() {
        return credentialsProfile;
    }

    public String getTopicArn() {
        return topicArn;
    }

    public String getTopicArnPrefix() {
        return topicArnPrefix;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public String getMessageGroupIdHeader() {
        return messageGroupIdHeader;
    }

    public String getMessageDeduplicationIdHeader() {
        return messageDeduplicationIdHeader;
    }

    public String getFifoDefaultGroupId() {
        return fifoDefaultGroupId;
    }
}
