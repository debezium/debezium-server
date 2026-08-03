/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.ydb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.fasterxml.jackson.databind.JsonNode;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

@QuarkusTest
@TestProfile(YdbMultiTableTestProfile.class)
@Timeout(120)
public class YdbMultiTableIT extends YdbITBase {

    @Test
    public void routesEachTableToItsOwnTopic() throws Exception {
        String customersTopic = YdbTestConfigSource.prefixedTopic(YdbTestConfigSource.DESTINATION_CUSTOMERS);
        String productsTopic = YdbTestConfigSource.prefixedTopic(YdbTestConfigSource.DESTINATION_PRODUCTS);

        try (TestUtils.TopicCapture customers = TestUtils.captureTopic(customersTopic, YdbTestConfigSource.CONSUMER);
                TestUtils.TopicCapture products = TestUtils.captureTopic(productsTopic, YdbTestConfigSource.CONSUMER)) {

            customers.awaitAtLeast(4);
            products.awaitAtLeast(1);

            assertOnlyTable(customers.snapshot(), "customers");
            assertOnlyTable(products.snapshot(), "products");
        }
    }

    private static void assertOnlyTable(List<TestUtils.CapturedMessage> messages, String table) throws Exception {
        assertThat(messages).isNotEmpty();
        for (TestUtils.CapturedMessage m : messages) {
            JsonNode pl = YdbMessageAssertions.payload(m.data());
            assertThat(pl.path("source").path("table").asText()).isEqualTo(table);
        }
    }
}
