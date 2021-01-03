package com.lu.table.factories;

import java.util.List;
import java.util.Map;

public interface ConcurrencyReadFactory {
    Map<String, String> requiredContext();

    List<String> supportedProperties();

    List<?> getConcurrencyReadList(Map<String, String> properties);
}
