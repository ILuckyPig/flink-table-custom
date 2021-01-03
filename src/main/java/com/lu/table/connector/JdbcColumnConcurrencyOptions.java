package com.lu.table.connector;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public class JdbcColumnConcurrencyOptions implements Serializable {
    private final String partitionColumnName;
    private final String partitionColumnType;
    private final List<?> concurrencyReadList;


    public JdbcColumnConcurrencyOptions(String partitionColumnName, String partitionColumnType, List<?> concurrencyReadList) {
        this.partitionColumnName = partitionColumnName;
        this.partitionColumnType = partitionColumnType;
        this.concurrencyReadList = concurrencyReadList;
    }

    public Optional<String> getPartitionColumnName() {
        return Optional.ofNullable(partitionColumnName);
    }

    public Optional<String> getPartitionColumnType() {
        return Optional.ofNullable(partitionColumnType);
    }

    public Optional<List<?>> getConcurrencyReadList() {
        return Optional.ofNullable(concurrencyReadList);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        protected String partitionColumnName;
        protected String partitionColumnType;
        protected List<?> concurrencyReadList;


        public Builder setPartitionColumnName(String partitionColumnName) {
            this.partitionColumnName = partitionColumnName;
            return this;
        }

        public Builder setPartitionColumnType(String partitionColumnType) {
            this.partitionColumnType = partitionColumnType;
            return this;
        }

        public Builder setConcurrencyReadList(List<?> concurrencyReadList) {
            this.concurrencyReadList = concurrencyReadList;
            return this;
        }

        public JdbcColumnConcurrencyOptions build() {
            return new JdbcColumnConcurrencyOptions(partitionColumnName, partitionColumnType, concurrencyReadList);
        }
    }
}
