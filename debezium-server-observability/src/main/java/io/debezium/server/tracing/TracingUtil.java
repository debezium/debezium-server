/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.tracing;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.runtime.BatchEvent;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;

public final class TracingUtil {

    public static final TextMapGetter<Headers> HEADERS_GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Headers headers) {
            final List<String> keys = new ArrayList<>();
            if (headers != null) {
                headers.forEach(header -> keys.add(header.key()));
            }
            return keys;
        }

        @Override
        public String get(Headers headers, String key) {
            if (headers == null) {
                return null;
            }
            final Header header = headers.lastWithName(key);
            return header == null || header.value() == null ? null : header.value().toString();
        }
    };

    private TracingUtil() {
    }

    public static Context tracingContext(BatchEvent record) {
        final SourceRecord sourceRecord = record.record();
        if (sourceRecord == null) {
            return Context.current();
        }
        return GlobalOpenTelemetry.getPropagators()
                .getTextMapPropagator()
                .extract(Context.current(), sourceRecord.headers(), HEADERS_GETTER);
    }
}
