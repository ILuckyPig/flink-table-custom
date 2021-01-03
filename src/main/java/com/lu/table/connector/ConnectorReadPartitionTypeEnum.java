package com.lu.table.connector;

public enum ConnectorReadPartitionTypeEnum {
    DEFAULT("default"),
    SPECIFIED_COLUMN("column");

    private final String value;

    ConnectorReadPartitionTypeEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
