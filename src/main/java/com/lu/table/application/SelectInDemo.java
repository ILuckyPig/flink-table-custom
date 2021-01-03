package com.lu.table.application;

import org.apache.calcite.plan.RelOptRule;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.calcite.CalciteConfig;
import org.apache.flink.table.plan.rules.FlinkRuleSets;

import java.util.ArrayList;
import java.util.List;

public class SelectInDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.create(environment);
        List<RelOptRule> list = new ArrayList<>();
        FlinkRuleSets.LOGICAL_OPT_RULES().forEach(i -> {

        });
        CalciteConfig calciteConfig = CalciteConfig.createBuilder()
                // .addDecoRuleSet(RuleSets.ofList())
                // .addNormRuleSet(RuleSets.ofList())
                // .addLogicalOptRuleSet(RuleSets.ofList())
                // .addLogicalRewriteRuleSet(RuleSets.ofList())
                // .addPhysicalOptRuleSet(RuleSets.ofList())
                // .replaceLogicalOptRuleSet(RuleSets.ofList(list))
                .build();
        tableEnvironment.getConfig().setPlannerConfig(calciteConfig);
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
                        "   'connector.read.partition.upper-bound'='2020-10-02'" +
                        ")"
        );
        tableEnvironment.executeSql(
                "CREATE TABLE weibo_status (" +
                        "   `date` DATE," +
                        "   mid BIGINT" +
                        ") WITH (" +
                        "   'connector.type'='jdbc-batch'," +
                        "   'connector.driver'='com.mysql.cj.jdbc.Driver'," +
                        "   'connector.url'='jdbc:mysql://localhost:3306/learn?serverTimezone=Asia/Shanghai'," +
                        "   'connector.username'='root'," +
                        "   'connector.password'='root'," +
                        "   'connector.table'='weibo_status'" +
                        ")"
        );
        Table table = tableEnvironment.sqlQuery("select * from weibo_status where mid in (select mid from rel_brand_commodity_weibo_status)");
        System.out.println(table.explain());
        // tableEnvironment.toDataSet(table, Row.class)
        //         .map(i -> {
        //             Thread.sleep(1000);
        //             return i;
        //         })
        //         .printOnTaskManager("demo");
        // environment.execute();
    }
}
