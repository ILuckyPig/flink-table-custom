package com.lu.table.connector.clickhouse.partitioner.factories;

import com.aliyun.flink.connector.clickhouse.table.internal.partitioner.ClickHousePartitioner;
import org.junit.Test;

import java.util.List;

public class PartitionerFactoryUtilTest {
    @Test
    public void test1() {
        List<PartitionerFactory> clickHousePartitioner = PartitionerFactoryUtil.findClickHousePartitioner();
        for (PartitionerFactory partitionerFactory : clickHousePartitioner) {
            System.out.println(partitionerFactory.factoryIdentifier());
        }
    }

    @Test
    public void test2() {
        PartitionerFactory toYYYYMM = PartitionerFactoryUtil.findAndCreateClickHousePartitioner("toYYYYMM");
        ClickHousePartitioner clickHousePartitioner = toYYYYMM.getClickHousePartitioner(null);
        System.out.println(clickHousePartitioner);
    }
}
