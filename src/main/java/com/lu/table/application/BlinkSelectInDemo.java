package com.lu.table.application;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class BlinkSelectInDemo {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        tableEnvironment.executeSql(
                "CREATE TABLE rel_brand_commodity_weibo_status (" +
                        "   `date` DATE," +
                        "   mid BIGINT," +
                        "   keyword STRING," +
                        "   proctime AS PROCTIME()" +
                        ") WITH (" +
                        "   'connector'='jdbc'," +
                        "   'driver'='com.mysql.cj.jdbc.Driver'," +
                        "   'url'='jdbc:mysql://localhost:3306/learn?serverTimezone=Asia/Shanghai'," +
                        "   'username'='root'," +
                        "   'password'='root'," +
                        "   'table-name'='rel_brand_commodity_weibo_status'," +
                        "   'scan.partition.column'='industryid'," +
                        "   'scan.partition.num'='5'," +
                        "   'scan.partition.lower-bound'='1'," +
                        "   'scan.partition.upper-bound'='29'" +
                        ")"
        );
        tableEnvironment.executeSql(
                "CREATE TABLE weibo_status (" +
                        "   `date` DATE," +
                        "   mid BIGINT," +
                        "   PRIMARY KEY (mid) NOT ENFORCED" +
                        ") WITH (" +
                        "   'connector'='jdbc'," +
                        "   'driver'='com.mysql.cj.jdbc.Driver'," +
                        "   'url'='jdbc:mysql://localhost:3306/learn?serverTimezone=Asia/Shanghai'," +
                        "   'username'='root'," +
                        "   'password'='root'," +
                        "   'table-name'='weibo_status'," +
                        "   'lookup.max-retries'='5'" +
                        ")"
        );
        tableEnvironment.executeSql(
                "CREATE TABLE print (" +
                        "   `date` DATE," +
                        "   mid BIGINT" +
                        ") WITH ('connector'='print')"
        );
        tableEnvironment.executeSql("INSERT INTO print SELECT rel.`date`,weibo_status.mid FROM rel_brand_commodity_weibo_status AS rel" +
                "   JOIN weibo_status FOR SYSTEM_TIME AS OF rel.proctime" +
                "   ON rel.mid = weibo_status.mid");
    }
}
