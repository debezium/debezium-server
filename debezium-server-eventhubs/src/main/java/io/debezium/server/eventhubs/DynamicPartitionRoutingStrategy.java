 package io.debezium.server.eventhubs;
 
 public enum DynamicPartitionRoutingStrategy {
        DEFAULT,
        KEY,
        PARTITIONID;

        public static DynamicPartitionRoutingStrategy fromString(String value) {
            try {
                return DynamicPartitionRoutingStrategy.valueOf(value.toUpperCase());
            } catch (IllegalArgumentException e) {
                return DEFAULT;
            }
        }
    }