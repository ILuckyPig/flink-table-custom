package com.lu.table.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class JdbcDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(4);
        environment.disableOperatorChaining();
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, fsSettings);

        tableEnvironment.executeSql(
                "CREATE TABLE rel_brand_commodity_weibo_status (" +
                        "   `date` DATE," +
                        "   mid BIGINT," +
                        "   keyword STRING" +
                        ") WITH (" +
                        "   'connector.type'='jdbc'," +
                        "   'connector.driver'='com.mysql.cj.jdbc.Driver'," +
                        "   'connector.url'='jdbc:mysql://localhost:3306/learn?serverTimezone=Asia/Shanghai'," +
                        "   'connector.username'='root'," +
                        "   'connector.password'='root'," +
                        "   'connector.table'='rel_brand_commodity_weibo_status'," +
                        "   'connector.read.partition.column'='industryid'," +
                        "   'connector.read.partition.num'='4'," +
                        "   'connector.read.partition.lower-bound'='1'," +
                        "   'connector.read.partition.upper-bound'='21'" +
                        // "   'connector.read.fetch-size'='100'" +
                        ")"
        );
        tableEnvironment.executeSql(
                "CREATE TABLE weibo_status (" +
                        "   `date` DATE," +
                        "   mid BIGINT" +
                        ") WITH (" +
                        "   'connector.type'='jdbc'," +
                        "   'connector.driver'='com.mysql.cj.jdbc.Driver'," +
                        "   'connector.url'='jdbc:mysql://localhost:3306/learn?serverTimezone=Asia/Shanghai'," +
                        "   'connector.username'='root'," +
                        "   'connector.password'='root'," +
                        "   'connector.table'='weibo_status'" +
                        ")"
        );
        Table table = tableEnvironment.sqlQuery("select * from weibo_status where mid in (select mid from rel_brand_commodity_weibo_status)");
        System.out.println(table.explain());
        // tableEnvironment.toAppendStream(table2, Row.class)
        //         .map(i -> {
        //             Thread.sleep(5 * 1000);
        //             return i;
        //         })
        //         .print();
        // environment.execute();
    }
}
