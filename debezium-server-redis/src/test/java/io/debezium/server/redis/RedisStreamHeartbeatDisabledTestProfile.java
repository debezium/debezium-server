/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.util.Map;

public class RedisStreamHeartbeatDisabledTestProfile extends RedisStreamTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        Map<String, String> config = super.getConfigOverrides();

        // Enable heartbeats with a short interval
        config.put("debezium.source.heartbeat.interval.ms", "50");

        // Set skip.heartbeat.messages to false to allow heartbeat messages to be stored
        config.put("debezium.sink.redis.skip.heartbeat.messages", "false");

        return config;
    }
}
