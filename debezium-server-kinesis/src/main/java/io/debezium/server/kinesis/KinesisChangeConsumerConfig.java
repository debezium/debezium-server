/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.kinesis;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Configuration fields for {@link KinesisChangeConsumer}.
 */
public class KinesisChangeConsumerConfig {

    public static final int MAX_BATCH_SIZE = 500;
    public static final int DEFAULT_RETRY_COUNT = 5;

    public static final Field REGION = Field.create("region")
            .withDisplayName("AWS Region")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("AWS region for the Kinesis stream.");

    public static final Field ENDPOINT = Field.create("endpoint")
            .withDisplayName("Kinesis Endpoint Override")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Optional Kinesis endpoint override (for local testing with LocalStack).");

    public static final Field CREDENTIALS_PROFILE = Field.create("credentials.profile")
            .withDisplayName("AWS Credentials Profile")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("AWS credentials profile name to use for authentication.");

    public static final Field BATCH_SIZE = Field.create("batch.size")
            .withDisplayName("Batch Size")
            .withType(ConfigDef.Type.INT)
            .withDefault(MAX_BATCH_SIZE)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum number of records to send in a single batch (max 500).");

    public static final Field DEFAULT_RETRIES = Field.create("default.retries")
            .withDisplayName("Default Retries")
            .withType(ConfigDef.Type.INT)
            .withDefault(DEFAULT_RETRY_COUNT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Maximum number of retry attempts for failed requests.");

    public static final Field NULL_KEY = Field.create("null.key")
            .withDisplayName("Null Key Value")
            .withType(ConfigDef.Type.STRING)
            .withDefault("default")
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Partition key to use when the record key is null.");

    // Instance fields
    private String region;
    private String endpoint;
    private String credentialsProfile;
    private int batchSize;
    private int maxRetries;
    private String nullKey;

    public KinesisChangeConsumerConfig(Configuration config) {
        init(config);
    }

    protected void init(Configuration config) {
        region = config.getString(REGION);
        endpoint = config.getString(ENDPOINT);
        credentialsProfile = config.getString(CREDENTIALS_PROFILE);
        batchSize = config.getInteger(BATCH_SIZE);
        maxRetries = config.getInteger(DEFAULT_RETRIES);
        nullKey = config.getString(NULL_KEY);
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

    public int getBatchSize() {
        return batchSize;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public String getNullKey() {
        return nullKey;
    }
}
