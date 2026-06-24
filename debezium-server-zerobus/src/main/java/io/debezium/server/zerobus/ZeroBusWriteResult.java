/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public record ZeroBusWriteResult(List<Boolean> acknowledged) {

    public ZeroBusWriteResult {
        acknowledged = List.copyOf(acknowledged);
    }

    public static ZeroBusWriteResult acknowledged(int recordCount) {
        return new ZeroBusWriteResult(Collections.nCopies(recordCount, true));
    }

    public static ZeroBusWriteResult partial(int recordCount, int acknowledgedCount) {
        List<Boolean> acknowledgements = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            acknowledgements.add(i < acknowledgedCount);
        }
        return new ZeroBusWriteResult(acknowledgements);
    }

    public boolean allAcknowledged(int expectedCount) {
        return acknowledged.size() == expectedCount && acknowledged.stream().allMatch(Boolean::booleanValue);
    }
}
