package com.lu.table.util;

import org.junit.Test;

import java.sql.Date;
import java.time.LocalDate;
import java.util.List;

public class DateUtilTest {
    @Test
    public void test() {
        List<java.sql.Date> dateList = DateUtil.getDateList(LocalDate.now().minusDays(5), LocalDate.now());
        System.out.println(dateList);
    }

    @Test
    public void test1() {
        int yearMonth = DateUtil.getYearMonth(Date.valueOf(LocalDate.now()));
        System.out.println(yearMonth);
    }

    @Test
    public void test2() {
        int year = DateUtil.getYear(Date.valueOf(LocalDate.now()));
        System.out.println(year);
    }
}
