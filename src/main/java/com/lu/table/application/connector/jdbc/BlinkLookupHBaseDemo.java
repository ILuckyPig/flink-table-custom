package com.lu.table.application.connector.jdbc;

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
                        "   industryid INT," +
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
                        "   'connector'='custom-jdbc'," +
                        "   'driver'='com.mysql.cj.jdbc.Driver'," +
                        "   'url'='jdbc:mysql://192.168.8.55:4000/mbase?serverTimezone=Asia/Shanghai&tinyInt1isBit=false'," +
                        "   'username'='edison'," +
                        "   'password'='edison'," +
                        "   'table-name'='rel_brand_commodity_weibo_status'," +
                        "   'scan.partition.column'='date'," +
                        "   'scan.partition.num'='2'," +
                        "   'scan.partition.lower-bound'='2019-11-15'," +
                        "   'scan.partition.upper-bound'='2019-11-15'," +
                        "   'filterable-fields'='industryid;brandid'" +
                        ")"
        );
        // tableEnvironment.executeSql(
        //         "CREATE TABLE weibo_status (" +
        //                 "   rowkey STRING," +
        //                 "   cf ROW<seg STRING>," +
        //                 "   PRIMARY KEY(rowkey) NOT ENFORCED" +
        //                 ") WITH (" +
        //                 "   'connector'='hbase-2.2'," +
        //                 "   'table-name'='mbase_dev:weibo_status'," +
        //                 "   'zookeeper.quorum'='hadoop-27:2181,hadoop-28:2181,hadoop-26:2181'" +
        //                 ")"
        // );
        // tableEnvironment.executeSql(
        //         "CREATE TABLE print (" +
        //                 "   `date` DATE," +
        //                 "   mid BIGINT," +
        //                 "   rowkey STRING," +
        //                 "   segment STRING" +
        //                 ") WITH (" +
        //                 "   'connector'='print'" +
        //                 ")"
        // );
        // tableEnvironment.executeSql("INSERT INTO print" +
        //         "   SELECT rel.`date`,rel.mid,weibo_status.rowkey,weibo_status.cf.seg FROM rel_brand_commodity_weibo_status AS rel" +
        //         "   JOIN weibo_status FOR SYSTEM_TIME AS OF rel.proctime" +
        //         "   ON rel.midRowKey = weibo_status.rowkey");
        tableEnvironment.executeSql("create table print (" +
                "   industryid INT," +
                "   `date` DATE," +
                "   mid BIGINT) WITH ('connector'='print')");
        tableEnvironment.executeSql("INSERT INTO print SELECT industryid,`date`,count(mid) " +
                "from rel_brand_commodity_weibo_status WHERE `date` between '2019-11-15' and '2019-11-15' and brandid > 16 " +
                "group by industryid,`date`");
    }
}
