package com.lu.table.application;

import com.lu.table.function.WeiboInterestsFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

public class CommodityFactApplication {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        // settings.toExecutorProperties().put(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE.key(), GlobalDataExchangeMode.ALL_EDGES_BLOCKING.toString());
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);
        Configuration configuration = new Configuration();
        configuration.setString(PipelineOptions.NAME, "test");
        tableEnvironment.getConfig().addConfiguration(configuration);
        tableEnvironment.createTemporaryFunction("weibo_interets_function", WeiboInterestsFunction.class);

        tableEnvironment.executeSql(
                "CREATE TABLE rel_brand_commodity_weibo_status (" +
                        "   `date` DATE," +
                        "   industryid INT," +
                        "   categoryid INT," +
                        "   brandid INT," +
                        "   commodityid BIGINT," +
                        "   mid BIGINT," +
                        "   uid BIGINT," +
                        "   repost_count INT," +
                        "   comment_count INT," +
                        "   attitude_count INT," +
                        "   interaction_count INT," +
                        "   iscooperate TINYINT," +
                        "   source TINYINT," +
                        "   proctime AS PROCTIME()" +
                        ") WITH (" +
                        "   'connector'='custom-jdbc'," +
                        "   'driver'='com.mysql.cj.jdbc.Driver'," +
                        "   'url'='jdbc:mysql://192.168.8.55:4000/mbase?serverTimezone=Asia/Shanghai&tinyInt1isBit=false'," +
                        "   'username'='edison'," +
                        "   'password'='edison'," +
                        "   'table-name'='rel_brand_commodity_weibo_status'," +
                        "   'scan.partition.column'='date'," +
                        "   'scan.partition.num'='1'," +
                        "   'scan.partition.lower-bound'='2019-11-13'," +
                        "   'scan.partition.upper-bound'='2019-11-13'," +
                        "   'filterable-fields'='industryid;brandid'" +
                        ")"
        );
        tableEnvironment.executeSql(
                "CREATE TABLE weibo_status (" +
                        "   `date` DATE," +
                        "   mid BIGINT," +
                        "   uid BIGINT," +
                        "   pos TINYINT," +
                        "   created_at TIMESTAMP," +
                        "   text STRING" +
                        ") WITH (" +
                        "   'connector'='jdbc'," +
                        "   'driver'='com.mysql.cj.jdbc.Driver'," +
                        "   'url'='jdbc:mysql://192.168.8.55:4000/mbase?serverTimezone=Asia/Shanghai&tinyInt1isBit=false'," +
                        "   'username'='edison'," +
                        "   'password'='edison'," +
                        "   'table-name'='weibo_status'" +
                        ")"
        );
        tableEnvironment.executeSql(
                "CREATE TABLE weibo_user (" +
                        "   id DECIMAL(19,0)," +
                        "   age VARCHAR(10)," +
                        "   gender VARCHAR(10)," +
                        "   city VARCHAR(20)," +
                        "   interests STRING," +
                        "   followers_count INT," +
                        "   verifiyType TINYINT," +
                        "   province STRING," +
                        "   friends_count INT," +
                        "   statuses_count INT," +
                        "   buy_power TINYINT," +
                        "   life_stage VARCHAR(10)" +
                        ") WITH (" +
                        "   'connector'='jdbc'," +
                        "   'driver'='com.mysql.cj.jdbc.Driver'," +
                        "   'url'='jdbc:mysql://192.168.8.55:4000/mbase?serverTimezone=Asia/Shanghai&tinyInt1isBit=false'," +
                        "   'username'='edison'," +
                        "   'password'='edison'," +
                        "   'table-name'='weibo_user'" +
                        ")"
        );
        tableEnvironment.executeSql(
                "CREATE TABLE report_date_commodity_status (\n" +
                        "    platform TINYINT,\n" +
                        "    `date` Date,\n" +
                        "    post_time TIMESTAMP,\n" +
                        "    industryid INTEGER,\n" +
                        "    categoryid INTEGER,\n" +
                        "    brandid INTEGER,\n" +
                        "    commodityid BIGINT,\n" +
                        "    mid BIGINT,\n" +
                        "    pos TINYINT,\n" +
                        "    interaction_count BIGINT,\n" +
                        "    follower_count BIGINT,\n" +
                        "    friends_count BIGINT,\n" +
                        "    statuses_count BIGINT,\n" +
                        "    repost_count BIGINT,\n" +
                        "    comment_count BIGINT,\n" +
                        "    attitude_count BIGINT,\n" +
                        "    uid BIGINT,\n" +
                        "    verifiy_type TINYINT,\n" +
                        "    age VARCHAR(10),\n" +
                        "    gender VARCHAR(10),\n" +
                        "    province VARCHAR(10),\n" +
                        "    city VARCHAR(10),\n" +
                        "    city_level VARCHAR(10),\n" +
                        "    buy_power TINYINT,\n" +
                        "    life_stage VARCHAR(20),\n" +
                        "    iscooperate TINYINT,\n" +
                        "    source TINYINT\n" +
                        ") WITH (\n" +
                        "    'connector'='clickhouse',\n" +
                        "    'url'='clickhouse://192.168.12.148:8123',\n" +
                        "    'username'='default',\n" +
                        "    'password'='',\n" +
                        "    'database-name'='mbase_dev',\n" +
                        "    'table-name'='report_date_commodity_status',\n" +
                        "    'sink.batch-size'='1',\n" +
                        "    'sink.max-retries'='3',\n" +
                        "    'sink.partition-strategy'='balanced'," +
                        "    'sink.flush-interval'='20000'\n" +
                        ")"
        );
        TableResult tableResult = tableEnvironment.executeSql("INSERT INTO report_date_commodity_status SELECT CAST(1 AS TINYINT),rel.`date`,created_at,rel.industryid,rel.categoryid,rel.brandid,\n" +
                "rel.commodityid,rel.mid,pos,rel.interaction_count,followers_count,friends_count,statuses_count,\n" +
                "rel.repost_count,rel.comment_count,rel.attitude_count,rel.uid,\n" +
                "verifiyType,\n" +
                "age,gender,city,city,city,buy_power,life_stage,rel.iscooperate,rel.source\n" +
                "FROM rel_brand_commodity_weibo_status AS rel\n" +
                "JOIN weibo_status FOR SYSTEM_TIME AS OF rel.proctime\n" +
                "ON rel.`date`=weibo_status.`date` AND rel.mid=weibo_status.mid\n" +
                "JOIN weibo_user FOR SYSTEM_TIME AS OF rel.proctime\n" +
                "ON rel.uid=weibo_user.id");
        tableResult.await();
    }
}
