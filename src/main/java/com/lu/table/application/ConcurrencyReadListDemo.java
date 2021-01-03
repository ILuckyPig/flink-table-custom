package com.lu.table.application;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

public class ConcurrencyReadListDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.create(environment);
        environment.setParallelism(4);

        tableEnvironment.executeSql(
                "CREATE TABLE rel_brand_commodity_weibo_status (" +
                        "   `date` DATE," +
                        "   mid BIGINT," +
                        "   keyword STRING" +
                        ") WITH (" +
                        "   'connector.type'='jdbc-batch'," +
                        "   'connector.driver'='com.mysql.cj.jdbc.Driver'," +
                        "   'connector.url'='jdbc:mysql://localhost:3306/learn?serverTimezone=Asia/Shanghai'," +
                        "   'connector.username'='root'," +
                        "   'connector.password'='root'," +
                        "   'connector.table'='rel_brand_commodity_weibo_status'," +
                        "   'connector.read.partition.column'='date'," +
                        "   'connector.read.partition.column.type'='date'," +
                        "   'connector.read.partition.lower-bound'='2020-10-01'," +
                        "   'connector.read.partition.upper-bound'='2020-10-10'" +
                        ")"
        );
        Table table = tableEnvironment.sqlQuery("select `date`,mid from rel_brand_commodity_weibo_status");
        DataSet<Row> rowDataSet = tableEnvironment.toDataSet(table, Row.class);
        rowDataSet
                .map(i -> {
                    Thread.sleep(1000);
                    return i;
                })
                .printOnTaskManager("demo");
        environment.execute();
    }
}
