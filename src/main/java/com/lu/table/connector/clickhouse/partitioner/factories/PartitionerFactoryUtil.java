package com.lu.table.connector.clickhouse.partitioner.factories;

import java.util.List;

public class PartitionerFactoryUtil {
    /**
     * Returns all clickhouse partitioner
     *
     * @return
     */
    public static List<PartitionerFactory> findClickHousePartitioner() {
        return PartitionerFactoryService.find(PartitionerFactory.class);
    }

    /**
     * Returns a clickhouse partitioner matching the descriptor.
     */
    @SuppressWarnings("unchecked")
    public static PartitionerFactory findAndCreateClickHousePartitioner(String shardingKey) {
        try {
            return PartitionerFactoryService.find(PartitionerFactory.class, shardingKey);
        } catch (Throwable t) {
            throw new RuntimeException("findAndCreateReadPartitionList failed.", t);
        }
    }
}
