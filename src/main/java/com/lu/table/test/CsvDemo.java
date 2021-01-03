package com.lu.table.test;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

public class CsvDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.create(environment);

        tableEnvironment.executeSql(
                "CREATE TABLE test (" +
                        "   id INT" +
                        ") WITH (" +
                        "   'connector.type'='filesystem'," +
                        "   'connector.path'='E:\\coding\\idea\\flink-learn\\src\\main\\resources\\test'," +
                        "   'format.type'='csv'," +
                        "   'format.fields.0.name'='id'," +
                        "   'format.fields.0.data-type'='INT'" +
                        ")"
        );
        Table table = tableEnvironment.sqlQuery("select * from test");
        tableEnvironment.toDataSet(table, Row.class).print();
    }
}
