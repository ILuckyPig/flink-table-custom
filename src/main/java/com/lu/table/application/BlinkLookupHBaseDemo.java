package com.lu.table.application;

import com.lu.table.function.MidRowKeyFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class BlinkLookupHBaseDemo {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);
        Configuration configuration = new Configuration();
        configuration.setBoolean(PipelineOptions.OPERATOR_CHAINING, false);
        configuration.setString(PipelineOptions.NAME, "test");
        tableEnvironment.getConfig().addConfiguration(configuration);
        tableEnvironment.createTemporaryFunction("midRowKeyFunction", MidRowKeyFunction.class);

        tableEnvironment.executeSql(
                "CREATE TABLE rel_brand_commodity_weibo_status (" +
                        "   `date` DATE," +
                        "   inudstryid INT," +
                        "   categoryid INT," +
                        "   brandid INT," +
                        "   commodityid INT," +
                        "   mid BIGINT," +
                        "   midRowKey AS midRowKeyFunction(mid)," +
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
                        "   'url'='jdbc:mysql://localhost:4000/mbase?serverTimezone=Asia/Shanghai&tinyInt1isBit=false'," +
                        "   'username'='edison'," +
                        "   'password'='edison'," +
                        "   'table-name'='rel_brand_commodity_weibo_status'," +
                        "   'scan.partition.column'='industryid'," +
                        "   'scan.partition.num'='3'," +
                        "   'scan.partition.lower-bound'='1'," +
                        "   'scan.partition.upper-bound'='5'" +
                        ")"
        );
        tableEnvironment.executeSql(
                "CREATE TABLE weibo_status (" +
                        "   rowkey STRING," +
                        "   cf ROW<seg STRING>," +
                        "   PRIMARY KEY(rowkey) NOT ENFORCED" +
                        ") WITH (" +
                        "   'connector'='hbase-2.2'," +
                        "   'table-name'='mbase_dev:weibo_status'," +
                        "   'zookeeper.quorum'='hadoop-27:2181,hadoop-28:2181,hadoop-26:2181'" +
                        ")"
        );
        tableEnvironment.executeSql(
                "CREATE TABLE print (" +
                        "   `date` DATE," +
                        "   mid BIGINT," +
                        "   rowkey STRING," +
                        "   segment STRING" +
                        ") WITH (" +
                        "   'connector'='print'" +
                        ")"
        );
        tableEnvironment.executeSql("INSERT INTO print" +
                "   SELECT rel.`date`,rel.mid,weibo_status.rowkey,weibo_status.cf.seg FROM rel_brand_commodity_weibo_status AS rel" +
                "   JOIN weibo_status FOR SYSTEM_TIME AS OF rel.proctime" +
                "   ON rel.midRowKey = weibo_status.rowkey");
    }
}
