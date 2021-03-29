package com.lu.table.connector.custom.jdbc;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

public class CustomJdbcReadOptions implements Serializable {
    private final String query;
    private final String partitionColumnName;
    private final Class<?> partitionColumnClass;
    private final String partitionLowerBound;
    private final String partitionUpperBound;
    private final Integer numPartitions;

    private final int fetchSize;
    private final boolean autoCommit;

    private CustomJdbcReadOptions(
            String query,
            String partitionColumnName,
            Class<?> partitionColumnClass,
            String partitionLowerBound,
            String partitionUpperBound,
            Integer numPartitions,
            int fetchSize,
            boolean autoCommit) {
        this.query = query;
        this.partitionColumnName = partitionColumnName;
        this.partitionColumnClass = partitionColumnClass;
        this.partitionLowerBound = partitionLowerBound;
        this.partitionUpperBound = partitionUpperBound;
        this.numPartitions = numPartitions;

        this.fetchSize = fetchSize;
        this.autoCommit = autoCommit;
    }

    public Optional<String> getQuery() {
        return Optional.ofNullable(query);
    }

    public Optional<String> getPartitionColumnName() {
        return Optional.ofNullable(partitionColumnName);
    }

    public Optional<Class<?>> getPartitionColumnClass() {
        return Optional.ofNullable(partitionColumnClass);
    }

    public Optional<String> getPartitionLowerBound() {
        return Optional.ofNullable(partitionLowerBound);
    }

    public Optional<String> getPartitionUpperBound() {
        return Optional.ofNullable(partitionUpperBound);
    }

    public Optional<Integer> getNumPartitions() {
        return Optional.ofNullable(numPartitions);
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public boolean getAutoCommit() {
        return autoCommit;
    }

    public static CustomJdbcReadOptions.Builder builder() {
        return new CustomJdbcReadOptions.Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof CustomJdbcReadOptions) {
            CustomJdbcReadOptions options = (CustomJdbcReadOptions) o;
            return Objects.equals(query, options.query) &&
                    Objects.equals(partitionColumnName, options.partitionColumnName) &&
                    Objects.equals(partitionColumnClass, options.partitionColumnClass) &&
                    Objects.equals(partitionLowerBound, options.partitionLowerBound) &&
                    Objects.equals(partitionUpperBound, options.partitionUpperBound) &&
                    Objects.equals(numPartitions, options.numPartitions) &&
                    Objects.equals(fetchSize, options.fetchSize) &&
                    Objects.equals(autoCommit, options.autoCommit);
        } else {
            return false;
        }
    }

    /**
     * Builder of {@link CustomJdbcReadOptions}.
     */
    public static class Builder {
        protected String query;
        protected String partitionColumnName;
        protected Class<?> partitionColumnClass;
        protected String partitionLowerBound;
        protected String partitionUpperBound;
        protected Integer numPartitions;

        protected int fetchSize = 0;
        protected boolean autoCommit = true;

        /**
         * optional, SQL query statement for this JDBC source.
         */
        public CustomJdbcReadOptions.Builder setQuery(String query) {
            this.query = query;
            return this;
        }

        /**
         * optional, name of the column used for partitioning the input.
         */
        public CustomJdbcReadOptions.Builder setPartitionColumnName(String partitionColumnName) {
            this.partitionColumnName = partitionColumnName;
            return this;
        }

        /**
         * optional, {@link Class} of the column used for partitioning the input.
         */
        public void setPartitionColumnClass(Class<?> partitionColumnClass) {
            this.partitionColumnClass = partitionColumnClass;
        }

        /**
         * optional, the smallest value of the first partition.
         */
        public CustomJdbcReadOptions.Builder setPartitionLowerBound(String partitionLowerBound) {
            this.partitionLowerBound = partitionLowerBound;
            return this;
        }

        /**
         * optional, the largest value of the last partition.
         */
        public CustomJdbcReadOptions.Builder setPartitionUpperBound(String partitionUpperBound) {
            this.partitionUpperBound = partitionUpperBound;
            return this;
        }

        /**
         * optional, the maximum number of partitions that can be used for parallelism in table reading.
         */
        public CustomJdbcReadOptions.Builder setNumPartitions(int numPartitions) {
            this.numPartitions = numPartitions;
            return this;
        }

        /**
         * optional, the number of rows to fetch per round trip.
         * default value is 0, according to the jdbc api, 0 means that fetchSize hint will be ignored.
         */
        public CustomJdbcReadOptions.Builder setFetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }

        /**
         * optional, whether to set auto commit on the JDBC driver.
         */
        public CustomJdbcReadOptions.Builder setAutoCommit(boolean autoCommit) {
            this.autoCommit = autoCommit;
            return this;
        }

        public CustomJdbcReadOptions build() {
            return new CustomJdbcReadOptions(
                    query, partitionColumnName, partitionColumnClass, partitionLowerBound, partitionUpperBound, numPartitions, fetchSize, autoCommit);
        }
    }
}
