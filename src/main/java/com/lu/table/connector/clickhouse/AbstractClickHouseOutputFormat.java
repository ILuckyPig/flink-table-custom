package com.lu.table.connector.clickhouse;

import com.aliyun.flink.connector.clickhouse.table.internal.ClickHouseStatementFactory;
import com.aliyun.flink.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
import com.aliyun.flink.connector.clickhouse.table.internal.converter.ClickHouseRowConverter;
import com.aliyun.flink.connector.clickhouse.table.internal.executor.ClickHouseBatchExecutor;
import com.aliyun.flink.connector.clickhouse.table.internal.executor.ClickHouseExecutor;
import com.aliyun.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import com.aliyun.flink.connector.clickhouse.table.internal.partitioner.ClickHousePartitioner;
import com.lu.table.connector.clickhouse.partitioner.factories.PartitionerFactory;
import com.lu.table.connector.clickhouse.partitioner.factories.PartitionerFactoryUtil;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public abstract class AbstractClickHouseOutputFormat extends RichOutputFormat<RowData> implements Flushable {

    public AbstractClickHouseOutputFormat() {
    }

    public void configure(Configuration parameters) {
    }

    public static class Builder {
        private static final Logger LOG = LoggerFactory.getLogger(AbstractClickHouseOutputFormat.Builder.class);
        private DataType[] fieldDataTypes;
        private ClickHouseOptions options;
        private String[] fieldNames;
        private Optional<UniqueConstraint> primaryKey;
        private TypeInformation<RowData> rowDataTypeInformation;

        public Builder() {
        }

        public AbstractClickHouseOutputFormat.Builder withOptions(ClickHouseOptions options) {
            this.options = options;
            return this;
        }

        public AbstractClickHouseOutputFormat.Builder withFieldDataTypes(DataType[] fieldDataTypes) {
            this.fieldDataTypes = fieldDataTypes;
            return this;
        }

        public AbstractClickHouseOutputFormat.Builder withFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public AbstractClickHouseOutputFormat.Builder withRowDataTypeInfo(TypeInformation<RowData> rowDataTypeInfo) {
            this.rowDataTypeInformation = rowDataTypeInfo;
            return this;
        }

        public AbstractClickHouseOutputFormat.Builder withPrimaryKey(Optional<UniqueConstraint> primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public AbstractClickHouseOutputFormat build() {
            Preconditions.checkNotNull(this.options);
            Preconditions.checkNotNull(this.fieldNames);
            Preconditions.checkNotNull(this.fieldDataTypes);
            LogicalType[] logicalTypes = Arrays.stream(this.fieldDataTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new);
            ClickHouseRowConverter converter = new ClickHouseRowConverter(RowType.of(logicalTypes));
            if (this.primaryKey.isPresent()) {
                LOG.warn("If primary key is specified, connector will be in UPSERT mode.");
                LOG.warn("You will have significant performance loss.");
            }

            return this.options.getWriteLocal() ? this.createShardOutputFormat(logicalTypes, converter) : this.createBatchOutputFormat(converter);
        }

        private ClickHouseBatchOutputFormat createBatchOutputFormat(ClickHouseRowConverter converter) {
            ClickHouseExecutor executor;
            if (this.primaryKey.isPresent() && !this.options.getIgnoreDelete()) {
                executor = ClickHouseExecutor.createUpsertExecutor(this.options.getTableName(), this.fieldNames,
                        this.listToStringArray(this.primaryKey.get().getColumns()), converter, this.options);
            } else {
                String sql = ClickHouseStatementFactory.getInsertIntoStatement(this.options.getTableName(), this.fieldNames);
                executor = new ClickHouseBatchExecutor(sql, converter, this.options.getFlushInterval(), this.options.getBatchSize(), this.options.getMaxRetries(), this.rowDataTypeInformation);
            }

            return new ClickHouseBatchOutputFormat(new ClickHouseConnectionProvider(this.options), executor, this.options);
        }

        private ClickHouseShardOutputFormat createShardOutputFormat(LogicalType[] logicalTypes, ClickHouseRowConverter converter) {
            String partitionStrategy = this.options.getPartitionStrategy();

            ClickHousePartitioner partitioner;
            switch (partitionStrategy) {
                case "balanced":
                    partitioner = ClickHousePartitioner.createBalanced();
                    break;
                case "hash":
                    partitioner = ClickHousePartitioner.createShuffle();
                    break;
                case "shuffle":
                    int index = Arrays.asList(this.fieldNames).indexOf(this.options.getPartitionKey());
                    if (index == -1) {
                        throw new IllegalArgumentException("Partition key `" + this.options.getPartitionKey() + "` not found in table schema");
                    }

                    RowData.FieldGetter getter = RowData.createFieldGetter(logicalTypes[index], index);
                    partitioner = ClickHousePartitioner.createHash(getter);
                    break;
                default:
                    // 根据partitionStrategy获得对应PartitionerFactory
                    PartitionerFactory partitionerFactory = PartitionerFactoryUtil.findAndCreateClickHousePartitioner(partitionStrategy);
                    if (partitionerFactory == null) {
                        throw new IllegalArgumentException("Unknown sink.partition-strategy `" + this.options.getPartitionStrategy() + "`");
                    }

                    int i = Arrays.asList(this.fieldNames).indexOf(this.options.getPartitionKey());
                    if (i == -1) {
                        throw new IllegalArgumentException("Partition key `" + this.options.getPartitionKey() + "` not found in table schema");
                    }
                    RowData.FieldGetter fieldGetter = RowData.createFieldGetter(logicalTypes[i], i);
                    partitioner = partitionerFactory.getClickHousePartitioner(fieldGetter);
                    break;
            }

            Optional keyFields;
            if (this.primaryKey.isPresent() && !this.options.getIgnoreDelete()) {
                keyFields = Optional.of(this.listToStringArray(this.primaryKey.get().getColumns()));
            } else {
                keyFields = Optional.empty();
            }

            return new ClickHouseShardOutputFormat(new ClickHouseConnectionProvider(this.options), this.fieldNames,
                    keyFields, converter, partitioner, this.options);
        }

        private String[] listToStringArray(List<String> lists) {
            if (lists == null) {
                return new String[0];
            } else {
                String[] keyFields = new String[lists.size()];
                for (int i = 0; i < lists.size(); i++) {
                    keyFields[i] = lists.get(i);
                }
                return keyFields;
            }
        }
    }
}
