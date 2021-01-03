package com.lu.table.factories;

import java.util.List;
import java.util.Map;

public class ConcurrencyReadFactoryUtil {
    /**
     * Returns a table source matching the descriptor.
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> findAndCreateConcurrencyReadList(Map<String, String> properties) {
        try {
            return (List<T>) ConcurrencyReadFactoryService
                    .find(ConcurrencyReadFactory.class, properties)
                    .getConcurrencyReadList(properties);
        } catch (Throwable t) {
            throw new RuntimeException("findAndCreateReadPartitionList failed.", t);
        }
    }
}
