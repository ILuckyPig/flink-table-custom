package com.lu.table.connector.clickhouse;

import com.aliyun.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

public class ClickHouseDynamicTableSink implements DynamicTableSink {
    private final TableSchema tableSchema;
    private final ClickHouseOptions options;

    public ClickHouseDynamicTableSink(ClickHouseOptions options, TableSchema tableSchema) {
        this.options = options;
        this.tableSchema = tableSchema;
    }

    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        this.validatePrimaryKey(requestedMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        Preconditions.checkState(ChangelogMode.insertOnly().equals(requestedMode)
                || this.tableSchema.getPrimaryKey().isPresent(),
                "please declare primary key for sink table when query contains update/delete record.");
    }

    public DynamicTableSink.SinkRuntimeProvider getSinkRuntimeProvider(DynamicTableSink.Context context) {
        AbstractClickHouseOutputFormat outputFormat = (new AbstractClickHouseOutputFormat.Builder())
                .withOptions(this.options).withFieldNames(this.tableSchema.getFieldNames())
                .withFieldDataTypes(this.tableSchema.getFieldDataTypes())
                .withPrimaryKey(this.tableSchema.getPrimaryKey())
                .withRowDataTypeInfo(context.createTypeInformation(this.tableSchema.toRowDataType()))
                .build();
        return OutputFormatProvider.of(outputFormat);
    }

    public DynamicTableSink copy() {
        return new ClickHouseDynamicTableSink(this.options, this.tableSchema);
    }

    public String asSummaryString() {
        return "custom-clickHouse";
    }
}
