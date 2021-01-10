package com.lu.table.application;

import com.lu.table.function.WordVoiceFlatMap;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class BlinkUDTFDemo {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);
        tableEnvironment.createTemporaryFunction("splitFunction", WordVoiceFlatMap.class);

        tableEnvironment.executeSql(
                "CREATE TABLE rel_brand_commodity_weibo_status (" +
                        "   `date` DATE," +
                        "   mid BIGINT," +
                        "   segments STRING," +
                        "   proctime AS PROCTIME()" +
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
                "CREATE TABLE print (" +
                        "   `date` DATE," +
                        "   mid BIGINT," +
                        "   term STRING," +
                        "   pos INT," +
                        "   real_frequency INT" +
                        ") WITH ('connector'='print')"
        );

        tableEnvironment.executeSql("INSERT INTO print SELECT `date`,mid,term, pos, f FROM rel_brand_commodity_weibo_status" +
                "   LEFT JOIN LATERAL TABLE(splitFunction(segments)) AS T(term, pos, f) ON TRUE" +
                "   WHERE segments IS NOT NULL AND segments <> ''");
    }
}
