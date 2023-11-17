/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.storage.redis.RedisClient;
import io.debezium.util.Collect;

public class RedisMemoryThresholdTest {
	private static final String _5MB = String.valueOf(5 * 1024 * 1024);
	private static final String _10MB = String.valueOf(10 * 1024 * 1024);
	private static final String _20MB = String.valueOf(20 * 1024 * 1024);
	private static final long RECORD_SIZE = 2048L;
	private static final int BUFFER_SIZE = 500;
	private static final int RATE_PER_SECOND = 1000;

	@Test
	public void testMemoryLimits() {
		Configuration config = Configuration.from(Collect.hashMapOf("debezium.sink.redis.address", "localhost",
				"debezium.sink.redis.rate.per.second", RATE_PER_SECOND));
		RedisMemoryThreshold redisMemoryThreshold = new RedisMemoryThreshold(new RedisClientImpl(_10MB, _20MB),
				new RedisStreamChangeConsumerConfig(config));
		for (int i = 0; i < 8; i++) {
			Assert.assertEquals(redisMemoryThreshold.checkMemory(RECORD_SIZE, BUFFER_SIZE, RATE_PER_SECOND), true);
		}
		Assert.assertEquals(redisMemoryThreshold.checkMemory(RECORD_SIZE, BUFFER_SIZE, RATE_PER_SECOND), false);
		redisMemoryThreshold.setRedisClient(new RedisClientImpl(_5MB, _20MB));
		Assert.assertEquals(redisMemoryThreshold.checkMemory(RECORD_SIZE, BUFFER_SIZE, RATE_PER_SECOND), true);
	}

	private static class RedisClientImpl implements RedisClient {

		private String infoMemory;

		private RedisClientImpl(String usedMemoryBytes, String maxMemoryBytes) {
			this.infoMemory = (usedMemoryBytes == null ? "" : "used_memory:" + usedMemoryBytes + "\n")
					+ (maxMemoryBytes == null ? "" : "maxmemory:" + maxMemoryBytes);
		}

		@Override
		public String info(String section) {
			return infoMemory;
		}

		@Override
		public void disconnect() {
		}

		@Override
		public void close() {
		}

		@Override
		public String xadd(String key, Map<String, String> hash) {
			return null;
		}

		@Override
		public List<String> xadd(List<SimpleEntry<String, Map<String, String>>> hashes) {
			return null;
		}

		@Override
		public List<Map<String, String>> xrange(String key) {
			return null;
		}

		@Override
		public long xlen(String key) {
			return 0;
		}

		@Override
		public Map<String, String> hgetAll(String key) {
			return null;
		}

		@Override
		public long hset(byte[] key, byte[] field, byte[] value) {
			return 0;
		}

		@Override
		public long waitReplicas(int replicas, long timeout) {
			return 0;
		}

		@Override
		public String clientList() {
			return null;
		}
	}
}
