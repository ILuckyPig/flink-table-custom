package com.lu.table.util;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class DateUtil {
    private static final SimpleDateFormat YEAR_MONTH_FORMAT = new SimpleDateFormat("yyyyMM");
    private static final SimpleDateFormat YEAR_FORMAT = new SimpleDateFormat("yyyy");
    private static final DateTimeFormatter YEAR_MONTH_FORMATTER = DateTimeFormatter.ofPattern("yyyyMM");
    private static final DateTimeFormatter YEAR_FORMATTER = DateTimeFormatter.ofPattern("yyyy");
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

    /**
     * {@link java.sql.Date} format to yyyyMM
     *
     * @param date
     * @return
     */
    public static int getYearMonth(Date date) {
        return Integer.parseInt(YEAR_MONTH_FORMAT.format(date));
    }

    /**
     * {@link java.sql.Date} format to yyyy
     *
     * @param date
     * @return
     */
    public static int getYear(Date date) {
        return Integer.parseInt(YEAR_FORMAT.format(date));
    }

    /**
     * {@link java.time.LocalDate} format to yyyyMM
     *
     * @param date
     * @return
     */
    public static int getYearMonth(LocalDate date) {
        return Integer.parseInt(date.format(YEAR_MONTH_FORMATTER));
    }

    /**
     * {@link java.time.LocalDate} format to yyyy
     *
     * @param date
     * @return
     */
    public static int getYear(LocalDate date) {
        return Integer.parseInt(date.format(YEAR_FORMATTER));
    }
}
