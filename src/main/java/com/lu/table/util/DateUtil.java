package com.lu.table.util;

import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class DateUtil {
    /**
     * get date list
     *
     * @param startDate start date
     * @param endDate end date
     * @return date list of day by day
     */
    public static List<java.sql.Date> getDateList(LocalDate startDate, LocalDate endDate) {
        List<java.sql.Date> list = new ArrayList<>();
        while (startDate.isBefore(endDate) || startDate.isEqual(endDate)) {
            list.add(Date.valueOf(startDate));
            startDate = startDate.plusDays(1);
        }
        return list;
    }
}
