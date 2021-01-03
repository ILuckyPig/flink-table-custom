package com.lu.table.connector;

import org.apache.flink.table.descriptors.JdbcValidator;

public class JdbcBatchValidator extends JdbcValidator {
    public static final String CONNECTOR_TYPE_VALUE_JDBC = "jdbc-batch";

    public static final String CONNECTOR_READ_PARTITION_TYPE = "connector.read.partition.type";

    public static final String CONNECTOR_READ_PARTITION_COLUMN_TYPE = "connector.read.partition.column.type";
}
