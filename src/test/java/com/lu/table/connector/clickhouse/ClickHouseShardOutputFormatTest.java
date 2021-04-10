package com.lu.table.connector.clickhouse;

import org.junit.Test;

import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClickHouseShardOutputFormatTest {
    @Test
    public void test1() {
        final Pattern PATTERN = Pattern.compile("Distributed\\('?(?<cluster>[a-zA-Z_][0-9a-zA-Z_]*)'?,\\s*'?(?<database>[a-zA-Z_][0-9a-zA-Z_]*)'?,\\s*'?(?<table>[a-zA-Z_][0-9a-zA-Z_]*)'?,\\s*(?<partition>[\\w|(]+\\)?)\\)?");
        Matcher matcher = PATTERN.matcher("Distributed('test_cluster', 'test', 'url_log_local', toYYYYMM(date))");
        matcher.find();
        String remoteCluster = matcher.group("cluster");
        String remoteDatabase = matcher.group("database");
        String remoteTable = matcher.group("table");
        String partitioner = matcher.group("partition");
        System.out.println(remoteCluster);
        System.out.println(remoteDatabase);
        System.out.println(remoteTable);
        System.out.println(partitioner);
    }

    @Test
    public void getConnection() throws ClassNotFoundException, SQLException {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        try (Connection connection = DriverManager.getConnection("jdbc:clickhouse://172.18.0.3:10124/test", "default", null)) {
            PreparedStatement statement = connection.prepareStatement("select * from user_access_log_local");
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                System.out.println(resultSet.getDate("date"));
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
    }
}
