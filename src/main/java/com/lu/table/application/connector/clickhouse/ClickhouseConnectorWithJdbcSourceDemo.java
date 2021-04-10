package com.lu.table.application.connector.clickhouse;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class ClickhouseConnectorWithJdbcSourceDemo {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        tableEnvironment.executeSql(
                "CREATE TABLE user_access_log_source (" +
                        "   `date` Date" +
                        "   ,access_time TIMESTAMP(3)" +
                        "   ,url String" +
                        "   ,uid Int" +
                        "   ,level Int" +
                        ") WITH (" +
                        "   'connector'='jdbc'," +
                        "   'driver'='com.mysql.cj.jdbc.Driver'," +
                        "   'url'='jdbc:mysql://localhost:3306/learn?serverTimezone=Asia/Shanghai&tinyInt1isBit=false'," +
                        "   'username'='root'," +
                        "   'password'='root'," +
                        "   'table-name'='user_access_log'" +
                        ")"
        );

        tableEnvironment.executeSql(
                "CREATE TABLE user_access_log (" +
                        "   `date` Date" +
                        "   ,access_time TIMESTAMP(3)" +
                        "   ,url String" +
                        "   ,uid Int" +
                        "   ,level Int" +
                        ") WITH (" +
                        "   'connector'='custom-clickhouse',\n" +
                        "   'url'='clickhouse://localhost:10123',\n" +
                        "   'username'='default',\n" +
                        "   'password'='',\n" +
                        "   'database-name'='test',\n" +
                        "   'table-name'='user_access_log',\n" +
                        "   'sink.write-local'='true',\n" +
                        "   'sink.batch-size'='1',\n" +
                        "   'sink.max-retries'='3',\n" +
                        "   'sink.partition-strategy'='toYYYYMM'," +
                        "   'sink.partition-key'='date'\n" +
                        ")"
        );

        tableEnvironment.executeSql("insert into user_access_log select * from user_access_log_source");
    }
}
