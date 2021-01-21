package com.lu.table.function;

import org.apache.flink.table.functions.ScalarFunction;

public class MidRowKeyFunction extends ScalarFunction {
    public String eval(Long mid) {
        return String.format("%03d|", mid % 1000) + mid;
    }
}
