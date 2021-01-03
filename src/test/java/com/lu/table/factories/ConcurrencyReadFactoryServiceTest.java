package com.lu.table.factories;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.lu.table.connector.JdbcBatchValidator.CONNECTOR_READ_PARTITION_COLUMN_TYPE;
import static org.apache.flink.table.descriptors.JdbcValidator.*;

public class ConcurrencyReadFactoryServiceTest {

    @Test
    public void testFind() {
        Map<String, String> map = new HashMap<>();
        map.put(CONNECTOR_READ_PARTITION_COLUMN_TYPE, "date");
        map.put(CONNECTOR_READ_PARTITION_COLUMN, "date");
        map.put(CONNECTOR_READ_PARTITION_LOWER_BOUND, "date");
        map.put(CONNECTOR_READ_PARTITION_UPPER_BOUND, "date");
        ConcurrencyReadFactory concurrencyReadFactory = ConcurrencyReadFactoryService.find(ConcurrencyReadFactory.class, map);
        System.out.println(concurrencyReadFactory);
    }

    @Test
    public void testFind2() {
        Map<String, String> map = new HashMap<>();
        map.put(CONNECTOR_READ_PARTITION_COLUMN_TYPE, "partition");
        map.put(CONNECTOR_READ_PARTITION_LOWER_BOUND, "date");
        map.put(CONNECTOR_READ_PARTITION_UPPER_BOUND, "date");
        ConcurrencyReadFactory concurrencyReadFactory = ConcurrencyReadFactoryService.find(ConcurrencyReadFactory.class, map);
        System.out.println(concurrencyReadFactory);
    }
}
