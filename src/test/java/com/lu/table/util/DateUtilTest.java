package com.lu.table.util;

import org.junit.Test;

import java.time.LocalDate;
import java.util.List;

public class DateUtilTest {
    @Test
    public void test() {
        List<java.sql.Date> dateList = DateUtil.getDateList(LocalDate.now().minusDays(5), LocalDate.now());
        System.out.println(dateList);
    }
}
