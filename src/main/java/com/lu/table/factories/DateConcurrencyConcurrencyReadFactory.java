package com.lu.table.factories;

import com.lu.table.util.DateUtil;
import org.apache.flink.util.Preconditions;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.lu.table.connector.JdbcBatchValidator.CONNECTOR_READ_PARTITION_COLUMN_TYPE;
import static org.apache.flink.table.descriptors.JdbcValidator.*;

public class DateConcurrencyConcurrencyReadFactory implements ConcurrencyReadFactory {
    public static final String IDENTIFIER = "date";

    @Override
    public Map<String, String> requiredContext() {
        final Map<String, String> map = new HashMap<>();
        map.put(CONNECTOR_READ_PARTITION_COLUMN_TYPE, IDENTIFIER);
        return map;
    }

    @Override
    public List<String> supportedProperties() {
        final List<String> properties = new ArrayList<>();

        properties.add(CONNECTOR_READ_PARTITION_COLUMN);
        properties.add(CONNECTOR_READ_PARTITION_LOWER_BOUND);
        properties.add(CONNECTOR_READ_PARTITION_UPPER_BOUND);

        return properties;
    }

    @Override
    public List<?> getConcurrencyReadList(Map<String, String> properties) {
        String start = properties.get(CONNECTOR_READ_PARTITION_LOWER_BOUND);
        String end = properties.get(CONNECTOR_READ_PARTITION_UPPER_BOUND);

        LocalDate startDate = null;
        LocalDate endDate = null;
        try {
            startDate = LocalDate.parse(start);
            endDate = LocalDate.parse(end);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Preconditions.checkArgument(startDate.isBefore(endDate) || startDate.isEqual(endDate), "minVal must not be larger than maxVal");
        return DateUtil.getDateList(startDate, endDate);
    }
}
