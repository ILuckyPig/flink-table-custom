package com.lu.table.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class WeiboInterestsFunction extends ScalarFunction {
    private final String[] str = {"…", ".", "♪", "。", "！", "、", "♥", "❤", "✿", "☆", "★", "※", "？"};

    public String[] eval(String interest) {
        if (StringUtils.isEmpty(interest)) {
            return new String[]{};
        }
        interest = interest.toLowerCase();
        for (String s : str) {
            interest = interest.replace(s, "");
        }
        interest = interest
                .replace(",", " ")
                .replace("，", " ");
        Set<String> interestSet = new HashSet<>(Arrays.asList(interest.split("\\s+")));
        if (interestSet.contains("null")){
            interestSet.remove("null");
        }
        return interestSet.toArray(new String[interestSet.size()]);
    }
}
