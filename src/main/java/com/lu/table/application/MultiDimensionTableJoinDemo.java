package com.lu.table.application;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

public class MultiDimensionTableJoinDemo {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        tableEnvironment.executeSql(
                "CREATE TABLE rel_brand_commodity_weibo_status (" +
                        "   `date` DATE," +
                        "   mid BIGINT," +
                        "   proctime AS PROCTIME()" +
                        ") WITH (" +
                        "   'connector'='jdbc'," +
                        "   'driver'='com.mysql.cj.jdbc.Driver'," +
                        "   'url'='jdbc:mysql://localhost:3306/learn?serverTimezone=Asia/Shanghai'," +
                        "   'username'='root'," +
                        "   'password'='root'," +
                        "   'table-name'='rel_brand_commodity_weibo_status'" +
                        ")"
        );

        tableEnvironment.executeSql(
                "CREATE TABLE weibo_status (" +
                        "   `date` DATE," +
                        "   mid BIGINT," +
                        "   uid BIGINT," +
                        "   PRIMARY KEY(`date`, mid) NOT ENFORCED" +
                        ") WITH (" +
                        "   'connector'='jdbc'," +
                        "   'driver'='com.mysql.cj.jdbc.Driver'," +
                        "   'url'='jdbc:mysql://localhost:3306/learn?serverTimezone=Asia/Shanghai'," +
                        "   'username'='root'," +
                        "   'password'='root'," +
                        "   'table-name'='weibo_status'" +
                        ")"
        );

        tableEnvironment.executeSql(
                "CREATE TABLE weibo_user (" +
                        "   id DECIMAL(19,0)," +
                        "   name VARCHAR(45)," +
                        "   PRIMARY KEY(id) NOT ENFORCED" +
                        ") WITH (" +
                        "   'connector'='jdbc'," +
                        "   'driver'='com.mysql.cj.jdbc.Driver'," +
                        "   'url'='jdbc:mysql://localhost:3306/learn?serverTimezone=Asia/Shanghai'," +
                        "   'username'='root'," +
                        "   'password'='root'," +
                        "   'table-name'='weibo_user'" +
                        ")"
        );

        Table table = tableEnvironment.sqlQuery("SELECT uid,PROCTIME() AS proctime FROM rel_brand_commodity_weibo_status AS rel" +
                "   JOIN weibo_status FOR SYSTEM_TIME AS OF rel.proctime" +
                "   ON rel.`date` = weibo_status.`date`"
        );

        tableEnvironment.createTemporaryView("rel_tmp", table);

        Table table1 = tableEnvironment.sqlQuery(
                "SELECT uid,name FROM rel_tmp" +
                        "   JOIN weibo_user FOR SYSTEM_TIME AS OF rel_tmp.proctime" +
                        "   ON rel_tmp.uid = weibo_user.id"
        );
        System.out.println(table1.explain());
    }
}
