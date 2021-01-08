package com.lu.table.application;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class BlinkSelectInDemo {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);
        Configuration configuration = new Configuration();
        configuration.setBoolean(PipelineOptions.OPERATOR_CHAINING, false);
        configuration.setString(PipelineOptions.NAME, "test");
        tableEnvironment.getConfig().addConfiguration(configuration);

        tableEnvironment.executeSql(
                "CREATE TABLE rel_brand_commodity_weibo_status (" +
                        "   `date` DATE," +
                        "   inudstryid INT," +
                        "   categoryid INT," +
                        "   brandid INT," +
                        "   commodityid INT," +
                        "   mid BIGINT," +
                        "   uid BIGINT," +
                        "   repost_count INT," +
                        "   comment_count INT," +
                        "   attitude_count INT," +
                        "   interaction_count INT," +
                        "   iscooperate TINYINT," +
                        "   proctime AS PROCTIME()" +
                        ") WITH (" +
                        "   'connector'='jdbc'," +
                        "   'driver'='com.mysql.cj.jdbc.Driver'," +
                        "   'url'='jdbc:mysql://172.17.162.17:3306/learn?serverTimezone=Asia/Shanghai&tinyInt1isBit=false'," +
                        "   'username'='root'," +
                        "   'password'='root'," +
                        "   'table-name'='rel_brand_commodity_weibo_status'," +
                        "   'scan.partition.column'='industryid'," +
                        "   'scan.partition.num'='4'," +
                        "   'scan.partition.lower-bound'='1'," +
                        "   'scan.partition.upper-bound'='29'" +
                        ")"
        );
        tableEnvironment.executeSql(
                "CREATE TABLE weibo_status (" +
                        "   `date` DATE," +
                        "   mid BIGINT," +
                        "   pos TINYINT," +
                        "   text STRING," +
                        "   PRIMARY KEY (`date`,mid) NOT ENFORCED" +
                        ") WITH (" +
                        "   'connector'='jdbc'," +
                        "   'driver'='com.mysql.cj.jdbc.Driver'," +
                        "   'url'='jdbc:mysql://172.17.162.17:3306/learn?serverTimezone=Asia/Shanghai&&tinyInt1isBit=false'," +
                        "   'username'='root'," +
                        "   'password'='root'," +
                        "   'table-name'='weibo_status'," +
                        "   'lookup.max-retries'='5'" +
                        ")"
        );
        tableEnvironment.executeSql(
                "CREATE TABLE weibo_user (" +
                        "   id DECIMAL(19,0)," +
                        "   age VARCHAR(45)," +
                        "   gender VARCHAR(45)," +
                        "   city VARCHAR(20)," +
                        "   interests STRING," +
                        "   followers_count INT," +
                        "   verifiyType TINYINT," +
                        "   province VARCHAR(20)," +
                        "   friends_count INT," +
                        "   statuses_count INT," +
                        "   buy_power TINYINT," +
                        "   life_stage VARCHAR(10)," +
                        "   PRIMARY KEY (id) NOT ENFORCED" +
                        ") WITH (" +
                        "   'connector'='jdbc'," +
                        "   'driver'='com.mysql.cj.jdbc.Driver'," +
                        "   'url'='jdbc:mysql://172.17.162.17:3306/learn?serverTimezone=Asia/Shanghai&&tinyInt1isBit=false'," +
                        "   'username'='root'," +
                        "   'password'='root'," +
                        "   'table-name'='weibo_user'," +
                        "   'lookup.max-retries'='5'" +
                        ")"
        );
        tableEnvironment.executeSql(
                "CREATE TABLE print (" +
                        "   `date` DATE," +
                        "   mid BIGINT" +
                        ") WITH ('connector'='print')"
        );
        tableEnvironment.executeSql("INSERT INTO print" +
                "   SELECT rel.`date`,count(weibo_status.mid) FROM rel_brand_commodity_weibo_status AS rel" +
                "   JOIN weibo_status FOR SYSTEM_TIME AS OF rel.proctime" +
                "   ON rel.`date` = weibo_status.`date` AND rel.mid = weibo_status.mid" +
                "   JOIN weibo_user FOR SYSTEM_TIME AS OF rel.proctime" +
                "   ON rel.uid = weibo_user.id group by rel.`date`");
    }
}
