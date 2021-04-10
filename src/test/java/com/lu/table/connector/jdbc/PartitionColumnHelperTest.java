package com.lu.table.connector.jdbc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import java.time.LocalDate;
import java.util.List;

public class PartitionColumnHelperTest {
    @Test
    public void testGetParameters() {
        // List<Tuple2<?, ?>> parameters1 = PartitionColumnHelper.getParameters(Integer.class, "1", "10", 3);
        // System.out.println(parameters1);
        List<Tuple2<?, ?>> parameters2 = PartitionColumnHelper.getParameters(LocalDate.class, "2019-01-01", "2019-01-02", 2);
        System.out.println(parameters2);
    }
}
