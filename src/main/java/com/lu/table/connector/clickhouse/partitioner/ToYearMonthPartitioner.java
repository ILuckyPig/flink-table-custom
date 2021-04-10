package com.lu.table.connector.clickhouse.partitioner;

import com.aliyun.flink.connector.clickhouse.table.internal.partitioner.ClickHousePartitioner;
import com.lu.table.connector.clickhouse.partitioner.factories.PartitionerFactory;
import com.lu.table.util.DateUtil;
import org.apache.flink.table.data.RowData;

import java.time.LocalDate;

public class ToYearMonthPartitioner implements ClickHousePartitioner, PartitionerFactory {
    private static final String IDENTIFIER = "toYYYYMM";

    private RowData.FieldGetter getter;

    public ToYearMonthPartitioner() {
    }

    public ToYearMonthPartitioner(RowData.FieldGetter getter) {
        this.getter = getter;
    }

    @Override
    public int select(RowData rowData, int numShards) {
        int epochDay = (int) this.getter.getFieldOrNull(rowData);
        LocalDate localDate = LocalDate.ofEpochDay(epochDay);
        return DateUtil.getYearMonth(localDate) % numShards;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public ClickHousePartitioner getClickHousePartitioner(RowData.FieldGetter getter) {
        return new ToYearMonthPartitioner(getter);
    }
}
