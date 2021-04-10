package com.lu.table.connector.clickhouse.partitioner.factories;

import com.aliyun.flink.connector.clickhouse.table.internal.partitioner.ClickHousePartitioner;
import org.apache.flink.table.data.RowData;

public interface PartitionerFactory {
    String factoryIdentifier();

    ClickHousePartitioner getClickHousePartitioner(RowData.FieldGetter getter);
}
