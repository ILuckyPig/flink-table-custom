package com.lu.table.connector;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatementImpl;
import org.apache.flink.connector.jdbc.table.JdbcTableSource;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class JdbcBatchTableSource implements BatchTableSource<Row>, StreamTableSource<Row>, SupportsFilterPushDown {
    private final JdbcOptions options;
    private final JdbcReadOptions readOptions;
    private final JdbcColumnConcurrencyOptions concurrencyOptions;
    private final TableSchema schema;

    // index of fields selected, null means that all fields are selected
    private final int[] selectFields;
    private final DataType producedDataType;

    private JdbcBatchTableSource(JdbcOptions options, JdbcReadOptions readOptions,
                                 JdbcColumnConcurrencyOptions concurrencyOptions, TableSchema schema) {
        this(options, readOptions, concurrencyOptions, schema, null);
    }

    private JdbcBatchTableSource(JdbcOptions options, JdbcReadOptions readOptions,
                                 JdbcColumnConcurrencyOptions concurrencyOptions, TableSchema schema, int[] selectFields) {
        this.options = options;
        this.readOptions = readOptions;
        this.concurrencyOptions = concurrencyOptions;
        this.schema = schema;

        this.selectFields = selectFields;

        final DataType[] schemaDataTypes = schema.getFieldDataTypes();
        final String[] schemaFieldNames = schema.getFieldNames();
        if (selectFields != null) {
            DataType[] dataTypes = new DataType[selectFields.length];
            String[] fieldNames = new String[selectFields.length];
            for (int i = 0; i < selectFields.length; i++) {
                dataTypes[i] = schemaDataTypes[selectFields[i]];
                fieldNames[i] = schemaFieldNames[selectFields[i]];
            }
            this.producedDataType =
                    TableSchema.builder().fields(fieldNames, dataTypes).build().toRowDataType();
        } else {
            this.producedDataType = schema.toRowDataType();
        }
    }

    @Override
    public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
        return execEnv.createInput(getInputFormat(), (RowTypeInfo) fromDataTypeToLegacyInfo(producedDataType));
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    public static JdbcBatchTableSource.Builder builder() {
        return new JdbcBatchTableSource.Builder();
    }

    private JdbcInputFormat getInputFormat() {
        final RowTypeInfo rowTypeInfo = (RowTypeInfo) fromDataTypeToLegacyInfo(producedDataType);
        JdbcInputFormat.JdbcInputFormatBuilder builder = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername(options.getDriverName())
                .setDBUrl(options.getDbURL())
                .setRowTypeInfo(new RowTypeInfo(rowTypeInfo.getFieldTypes(), rowTypeInfo.getFieldNames()));
        options.getUsername().ifPresent(builder::setUsername);
        options.getPassword().ifPresent(builder::setPassword);

        if (readOptions.getFetchSize() != 0) {
            builder.setFetchSize(readOptions.getFetchSize());
        }
        String query = getBaseQueryStatement(rowTypeInfo);

        setParametersProvider(builder, query);

        return builder.finish();
    }

    private void setParametersProvider(JdbcInputFormat.JdbcInputFormatBuilder builder, String query) {
        final JdbcDialect dialect = options.getDialect();

        if (concurrencyOptions.getPartitionColumnName().isPresent() && concurrencyOptions.getPartitionColumnType().isPresent()) {
            List<?> ConcurrencyReadList = concurrencyOptions.getConcurrencyReadList().get();
            builder.setParametersProvider(new JdbcParameterValuesProvider() {
                @Override
                public Serializable[][] getParameterValues() {
                    Serializable[][] parameters = new Serializable[ConcurrencyReadList.size()][];
                    for (int i = 0; i < parameters.length; i++) {
                        parameters[i] = new Serializable[]{(Serializable) ConcurrencyReadList.get(i)};
                    }
                    return parameters;
                }
            });
            query += " WHERE " +
                    dialect.quoteIdentifier(concurrencyOptions.getPartitionColumnName().get()) +
                    " = ?";

        } else if (readOptions.getPartitionColumnName().isPresent()) {
            long lowerBound = readOptions.getPartitionLowerBound().get();
            long upperBound = readOptions.getPartitionUpperBound().get();
            int numPartitions = readOptions.getNumPartitions().get();
            builder.setParametersProvider(
                    new JdbcNumericBetweenParametersProvider(lowerBound, upperBound).ofBatchNum(numPartitions));
            query += " WHERE " +
                    dialect.quoteIdentifier(readOptions.getPartitionColumnName().get()) +
                    " BETWEEN ? AND ?";
        }
        builder.setQuery(query);
    }

    private String getBaseQueryStatement(RowTypeInfo rowTypeInfo) {
        return readOptions.getQuery().orElseGet(() ->
                FieldNamedPreparedStatementImpl.parseNamedStatement(
                        options.getDialect().getSelectFromStatement(options.getTableName(), rowTypeInfo.getFieldNames(), new String[0]),
                        new HashMap<>()
                )
        );
    }

    @Override
    public DataType getProducedDataType() {
        return producedDataType;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return null;
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        return null;
    }

    /**
     * Builder for a {@link JdbcTableSource}.
     */
    public static class Builder {
        private JdbcOptions options;
        private JdbcReadOptions readOptions;
        private JdbcColumnConcurrencyOptions concurrencyOptions;
        protected TableSchema schema;

        /**
         * required, jdbc options.
         */
        public JdbcBatchTableSource.Builder setOptions(JdbcOptions options) {
            this.options = options;
            return this;
        }

        /**
         * optional, scan related options.
         * {@link JdbcReadOptions} will be only used for {@link StreamTableSource}.
         */
        public JdbcBatchTableSource.Builder setReadOptions(JdbcReadOptions readOptions) {
            this.readOptions = readOptions;
            return this;
        }

        public JdbcBatchTableSource.Builder setConcurrencyOptions(JdbcColumnConcurrencyOptions concurrencyOptions) {
            this.concurrencyOptions = concurrencyOptions;
            return this;
        }

        /**
         * required, table schema of this table source.
         */
        public JdbcBatchTableSource.Builder setSchema(TableSchema schema) {
            this.schema = JdbcTypeUtil.normalizeTableSchema(schema);
            return this;
        }

        /**
         * Finalizes the configuration and checks validity.
         *
         * @return Configured JdbcTableSource
         */
        public JdbcBatchTableSource build() {
            checkNotNull(options, "No options supplied.");
            checkNotNull(schema, "No schema supplied.");
            if (readOptions == null) {
                readOptions = JdbcReadOptions.builder().build();
            }
            if (concurrencyOptions == null) {
                concurrencyOptions = JdbcColumnConcurrencyOptions.builder().build();
            }
            return new JdbcBatchTableSource(options, readOptions, concurrencyOptions, schema);
        }
    }
}
