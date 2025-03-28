/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.instructlab;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.server.instructlab.InstructLabSinkConsumer.MappingValue;

/**
 * Unit tests for the {@link MappingValue} class.
 *
 * @author Chris Cranford
 */
public class MappingValueTest {

    @Test
    public void testMappingValueFromConstant() {
        final MappingValue value = MappingValue.from("/domain/path1/path2/abc");
        assertThat(value.isHeader()).isFalse();
        assertThat(value.isField()).isFalse();
        assertThat(value.isConstant()).isTrue();
        assertThat(value.getValue()).isEqualTo("/domain/path1/path2/abc");
    }

    @Test
    public void testMappingValueFromHeader() {
        final MappingValue value = MappingValue.from("header:h1");
        assertThat(value.isHeader()).isTrue();
        assertThat(value.isField()).isFalse();
        assertThat(value.isConstant()).isFalse();
        assertThat(value.getValue()).isEqualTo("h1");
    }

    @Test
    public void testMappingValueFromEmptyHeader() {
        final MappingValue value = MappingValue.from("header:");
        assertThat(value.isHeader()).isTrue();
        assertThat(value.isField()).isFalse();
        assertThat(value.isConstant()).isFalse();
        assertThat(value.getValue()).isEmpty();
    }

    @Test
    public void testMappingValueFromFieldAllTopics() {
        final MappingValue value = MappingValue.from("value:xyz");
        assertThat(value.isHeader()).isFalse();
        assertThat(value.isField()).isTrue();
        assertThat(value.isConstant()).isFalse();
        assertThat(value.getValue()).isEqualTo("xyz");
    }
}
