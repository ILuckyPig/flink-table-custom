package com.lu.table.factories;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.lu.table.connector.JdbcBatchValidator.CONNECTOR_READ_PARTITION_COLUMN_TYPE;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_READ_PARTITION_LOWER_BOUND;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_READ_PARTITION_UPPER_BOUND;

public class PartitionsConcurrencyConcurrencyReadFactory implements ConcurrencyReadFactory {
    public static final String IDENTIFIER = "partition";

    @Override
    public Map<String, String> requiredContext() {
        final Map<String, String> map = new HashMap<>();
        map.put(CONNECTOR_READ_PARTITION_COLUMN_TYPE, IDENTIFIER);
        return map;
    }

    @Override
    public List<String> supportedProperties() {
        final List<String> properties = new ArrayList<>();

        properties.add(CONNECTOR_READ_PARTITION_LOWER_BOUND);
        properties.add(CONNECTOR_READ_PARTITION_UPPER_BOUND);

        return properties;
    }

    @Override
    public List<?> getConcurrencyReadList(Map<String, String> properties) {
        return null;
    }
}
