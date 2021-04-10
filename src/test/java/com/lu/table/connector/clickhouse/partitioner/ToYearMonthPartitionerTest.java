package com.lu.table.connector.clickhouse.partitioner;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DateType;
import org.junit.Test;

import java.sql.Date;
import java.time.LocalDate;

public class ToYearMonthPartitionerTest {
    @Test
    public void test() {
        RowData.FieldGetter fieldGetter = RowData.createFieldGetter(new DateType(false), 0);
        GenericRowData genericRowData = new GenericRowData(1);
        genericRowData.setField(0, Date.valueOf(LocalDate.now()));

        ToYearMonthPartitioner partitioner = new ToYearMonthPartitioner(fieldGetter);
        System.out.println(partitioner.select(genericRowData, 2));
    }
}
