package com.lu.table.application.connector.clickhouse;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class ClickhouseConnectorDemo {
    public static void main(String[] args) {
        String filePath = System.getProperty("user.dir") + "/src/main/resources/data/clickhouse-source-data";
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        tableEnvironment.executeSql(
                "CREATE TABLE file_source (" +
                        "   `date` Date" +
                        "   ,access_time TIMESTAMP(3)" +
                        "   ,url String" +
                        "   ,uid Int" +
                        "   ,level Int" +
                        ") WITH (" +
                        "   'connector'='filesystem'," +
                        "   'path'='file:///" + filePath + "'," +
                        "   'format'='csv'" +
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
                        // "   'sink.partition-strategy'='hash'," +
                        "   'sink.partition-key'='date'\n" +
                        ")"
        );

        tableEnvironment.executeSql("insert into user_access_log select * from file_source");
    }
}
