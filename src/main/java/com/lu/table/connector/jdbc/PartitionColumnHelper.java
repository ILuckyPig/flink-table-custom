package com.lu.table.connector.jdbc;

import org.apache.flink.api.java.tuple.Tuple2;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitionColumnHelper {
    private static final Map<Class<?>, String> VALUE_MAP = new HashMap<>();
    static {
        VALUE_MAP.put(Byte.class, Byte.class.getSimpleName());
        VALUE_MAP.put(Short.class, Short.class.getSimpleName());
        VALUE_MAP.put(Integer.class, Integer.class.getSimpleName());
        VALUE_MAP.put(Long.class, Long.class.getSimpleName());
        VALUE_MAP.put(LocalDate.class, LocalDate.class.getSimpleName());
    }
    private static final String VALUES = String.join(",", VALUE_MAP.values());

    public enum PartitionColumnTypeEnum {
        BYTE(Byte.class.getSimpleName()),
        SHORT(Short.class.getSimpleName()),
        INT(Integer.class.getSimpleName()),
        LONG(Long.class.getSimpleName()),
        DATE(LocalDate.class.getSimpleName());

        PartitionColumnTypeEnum(String value) {
        }

        public static PartitionColumnTypeEnum get(Class<?> clazz) {
            String value = VALUE_MAP.get(clazz);
            PartitionColumnTypeEnum columnTypeEnum;
            switch (value) {
                case "Byte":
                    columnTypeEnum = BYTE;
                    break;
                case "Short":
                    columnTypeEnum = SHORT;
                    break;
                case "Integer":
                    columnTypeEnum = INT;
                    break;
                case "Long":
                    columnTypeEnum = LONG;
                    break;
                case "LocalDate":
                    columnTypeEnum = DATE;
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + value);
            }
            return columnTypeEnum;
        }
    }

    public static boolean checkType(Class<?> value) {
        return VALUE_MAP.containsKey(value);
    }

    public static String getValues() {
        return VALUES;
    }

    public static List<Tuple2<?, ?>> getParameters(Class<?> clazz, String lowerBound, String upperBound, int numPartitions) {
        PartitionColumnTypeEnum partitionColumnTypeEnum = PartitionColumnTypeEnum.get(clazz);
        switch (partitionColumnTypeEnum) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return getParameters(Integer.parseInt(lowerBound), Integer.parseInt(upperBound), numPartitions);
            case DATE:
                return getParameters(LocalDate.parse(lowerBound), LocalDate.parse(upperBound), numPartitions);
            default:
                throw new IllegalArgumentException(String.format("could not create new instance for %s, %s",
                        lowerBound, upperBound));
        }
    }

    private static List<Tuple2<?, ?>> getParameters(long lower, long upper, int numPartitions) {
        long maxElemCount = (upper - lower) + 1;
        if (numPartitions > maxElemCount) {
            numPartitions = (int) maxElemCount;
        }
        int batchNum = numPartitions;
        long batchSize = new Double(Math.ceil((double) maxElemCount / batchNum)).longValue();
        long bigBatchNum = maxElemCount - (batchSize - 1) * batchNum;
        List<Tuple2<?, ?>> list = new ArrayList<>();
        long start = lower;
        for (int i = 0; i < batchNum; i++) {
            long end = start + batchSize - 1 - (i >= bigBatchNum ? 1 : 0);
            list.add(Tuple2.of(start, end));
            start = end + 1;
        }
        return list;
    }

    private static List<Tuple2<?, ?>> getParameters(LocalDate lower, LocalDate upper, int numPartitions) {
        long days = lower.until(upper, ChronoUnit.DAYS) + 1;
        if (numPartitions > days) {
            numPartitions = (int) days;
        }
        int batchNum = numPartitions;
        long batchSize = new Double(Math.ceil((double) days / batchNum)).longValue();
        long bigBatchNum = days - (batchSize - 1) * batchNum;
        List<Tuple2<?, ?>> list = new ArrayList<>();
        LocalDate start = lower;
        for (int i = 0; i < batchNum; i++) {
            LocalDate end = start.plusDays(batchSize).minusDays(1).minusDays((i >= bigBatchNum ? 1 : 0));
            list.add(Tuple2.of(start, end));
            start = end.plusDays(1);
        }
        return list;
    }
}
