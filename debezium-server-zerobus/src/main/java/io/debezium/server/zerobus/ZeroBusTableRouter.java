/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.zerobus;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;

/**
 * Maps Debezium destinations to Unity Catalog table identifiers without Kafka Connect SMTs.
 */
public class ZeroBusTableRouter {

    private static final Pattern UC_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private final ZeroBusSinkConfig config;
    private final Pattern regex;

    public ZeroBusTableRouter(ZeroBusSinkConfig config) {
        this.config = config;
        regex = config.getTableMappingRegex() == null ? null : Pattern.compile(config.getTableMappingRegex());
    }

    public String route(ChangeEvent<Object, Object> record) {
        String destination = record.destination();
        if (destination == null || destination.isBlank()) {
            throw new DebeziumException("ZeroBus table routing requires a non-empty Debezium destination");
        }

        String target = switch (config.getTableMappingMode()) {
            case ZeroBusSinkConfig.TABLE_MAPPING_SOURCE -> sourceMapping(destination);
            case ZeroBusSinkConfig.TABLE_MAPPING_EXPLICIT -> explicitMapping(destination, config.getTableMappingOverrides());
            case ZeroBusSinkConfig.TABLE_MAPPING_REGEX_MODE -> regexMapping(destination);
            default -> throw new DebeziumException("Unsupported ZeroBus table mapping mode '" + config.getTableMappingMode() + "'");
        };
        validateUnityCatalogTable(target);
        return target;
    }

    private String sourceMapping(String destination) {
        String table = destination.substring(destination.lastIndexOf('.') + 1);
        return config.getTableMappingDefaultCatalog() + "." + config.getTableMappingDefaultSchema() + "." + table;
    }

    private String explicitMapping(String destination, Map<String, String> overrides) {
        String target = overrides.get(destination);
        if (target == null) {
            throw new DebeziumException("No ZeroBus table mapping override found for Debezium destination '" + destination + "'");
        }
        return target;
    }

    private String regexMapping(String destination) {
        Matcher matcher = regex.matcher(destination);
        if (!matcher.matches()) {
            throw new DebeziumException("Debezium destination '" + destination + "' does not match ZeroBus table mapping regex");
        }
        return matcher.replaceAll(config.getTableMappingReplacement());
    }

    static void validateUnityCatalogTable(String table) {
        String[] parts = table == null ? new String[0] : table.split("\\.", -1);
        if (parts.length != 3) {
            throw new DebeziumException("ZeroBus target table must be a Unity Catalog identifier '<catalog>.<schema>.<table>': " + table);
        }
        for (String part : parts) {
            if (!UC_IDENTIFIER.matcher(part).matches()) {
                throw new DebeziumException("Invalid ZeroBus Unity Catalog identifier part '" + part + "' in table '" + table + "'");
            }
        }
    }
}
