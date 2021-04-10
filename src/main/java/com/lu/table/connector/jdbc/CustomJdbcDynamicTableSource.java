package com.lu.table.connector.jdbc;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcRowDataInputFormat;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.*;

/**
 * A {@link DynamicTableSource} for JDBC.
 */
@Internal
public class CustomJdbcDynamicTableSource implements ScanTableSource, SupportsProjectionPushDown, SupportsFilterPushDown {
    private static final Logger LOG = LoggerFactory.getLogger(CustomJdbcDynamicTableSource.class);

    private final JdbcOptions options;
    private final CustomJdbcReadOptions readOptions;
    private TableSchema physicalSchema;
    private final String dialectName;
    private List<ResolvedExpression> filterPredicates;
    private final Set<String> filterableFields;

    public CustomJdbcDynamicTableSource(
            JdbcOptions options,
            CustomJdbcReadOptions readOptions,
            List<ResolvedExpression> filterPredicates,
            Set<String> filterableFields,
            TableSchema physicalSchema) {
        this.options = options;
        this.readOptions = readOptions;
        this.filterPredicates = filterPredicates;
        this.filterableFields = filterableFields;
        this.physicalSchema = physicalSchema;
        this.dialectName = options.getDialect().dialectName();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final JdbcRowDataInputFormat.Builder builder = JdbcRowDataInputFormat.builder()
                .setDrivername(options.getDriverName())
                .setDBUrl(options.getDbURL())
                .setUsername(options.getUsername().orElse(null))
                .setPassword(options.getPassword().orElse(null))
                .setAutoCommit(readOptions.getAutoCommit());

        if (readOptions.getFetchSize() != 0) {
            builder.setFetchSize(readOptions.getFetchSize());
        }
        final JdbcDialect dialect = options.getDialect();
        StringBuilder query = new StringBuilder(dialect.getSelectFromStatement(
                options.getTableName(), physicalSchema.getFieldNames(), new String[0]));
        if (readOptions.getPartitionColumnName().isPresent()) {
            Class<?> partitionColumnClass = readOptions.getPartitionColumnClass().get();

            String lowerBound = readOptions.getPartitionLowerBound().get();
            String upperBound = readOptions.getPartitionUpperBound().get();
            int numPartitions = readOptions.getNumPartitions().get();
            List<Tuple2<?, ?>> parameters = PartitionColumnHelper.getParameters(partitionColumnClass, lowerBound,
                    upperBound, numPartitions);
            builder.setParametersProvider(() -> {
                Serializable[][] params = new Serializable[parameters.size()][2];
                for (int i = 0; i < parameters.size(); i++) {
                    Tuple2<?, ?> tuple2 = parameters.get(i);
                    params[i] = new Serializable[]{tuple2.f0.toString(), tuple2.f1.toString()};
                }
                return params;
            });
            query.append(" WHERE ").append(dialect.quoteIdentifier(readOptions.getPartitionColumnName().get())).append(" BETWEEN ? AND ?");
        }

        appendPushDownSql(query, dialect);

        LOG.info("Execute sql => {}", query.toString());

        builder.setQuery(query.toString());
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        builder.setRowConverter(dialect.getRowConverter(rowType));
        builder.setRowDataTypeInfo(
                runtimeProviderContext.createTypeInformation(physicalSchema.toRowDataType()));

        return InputFormatProvider.of(builder.build());
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.physicalSchema = TableSchemaUtils.projectSchema(physicalSchema, projectedFields);
    }

    @Override
    public DynamicTableSource copy() {
        return new CustomJdbcDynamicTableSource(options, readOptions, filterPredicates, filterableFields, physicalSchema);
    }

    @Override
    public String asSummaryString() {
        return "JDBC:" + dialectName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CustomJdbcDynamicTableSource)) {
            return false;
        }
        CustomJdbcDynamicTableSource that = (CustomJdbcDynamicTableSource) o;
        return Objects.equals(options, that.options) &&
                Objects.equals(readOptions, that.readOptions) &&
                Objects.equals(physicalSchema, that.physicalSchema) &&
                Objects.equals(filterableFields, this.filterableFields) &&
                Objects.equals(filterPredicates, this.filterPredicates) &&
                Objects.equals(dialectName, that.dialectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(options, readOptions, physicalSchema, dialectName, filterableFields, filterPredicates);
    }

    @Override
    public boolean supportsNestedProjection() {
        // JDBC doesn't support nested projection
        return false;
    }

    /**
     * 拼接下推sql
     *
     * @param query
     * @param dialect
     */
    public void appendPushDownSql(StringBuilder query, JdbcDialect dialect) {
        for (ResolvedExpression filterPredicate : this.filterPredicates) {
            CallExpression predicate = (CallExpression) filterPredicate;
            FunctionDefinition functionDefinition = predicate.getFunctionDefinition();
            String function = convert(functionDefinition);
            if (function != null) {
                List<ResolvedExpression> resolvedChildren = predicate.getResolvedChildren();
                assert resolvedChildren.size() == 2;
                FieldReferenceExpression head = (FieldReferenceExpression) resolvedChildren.get(0);
                ValueLiteralExpression last = (ValueLiteralExpression) resolvedChildren.get(1);
                Optional<?> valueAs = last.getValueAs(last.getOutputDataType().getConversionClass());

                if (query.indexOf("WHERE") == -1) {
                    query.append(" WHERE ");
                } else {
                    query.append(" AND ");
                }
                query.append(dialect.quoteIdentifier(head.getName()))
                        .append(" ").append(function)
                        .append(" '").append(valueAs.get().toString()).append("'");
            }
        }
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        List<ResolvedExpression> acceptedFilters = new ArrayList<>();
        List<ResolvedExpression> remainingFilters = new ArrayList<>();
        for (ResolvedExpression expr : filters) {
            if (shouldPushDown(expr, filterableFields)) {
                acceptedFilters.add(expr);
            } else {
                remainingFilters.add(expr);
            }
        }
        this.filterPredicates = acceptedFilters;
        return Result.of(acceptedFilters, remainingFilters);
    }

    /**
     * 将需要下推的{@link FunctionDefinition}转为sql运算符
     *
     * @param functionDefinition
     * @return sql运算符
     */
    public static String convert(FunctionDefinition functionDefinition) {
        if (LESS_THAN.equals(functionDefinition)) {
            return "<";
        } else if (LESS_THAN_OR_EQUAL.equals(functionDefinition)) {
            return "<=";
        } else if (EQUALS.equals(functionDefinition)) {
            return "=";
        } else if (GREATER_THAN.equals(functionDefinition)) {
            return ">";
        } else if (GREATER_THAN_OR_EQUAL.equals(functionDefinition)) {
            return ">=";
        }
        return null;
    }

    /**
     * 判断条件表达式是否可以下推
     *
     * @param expr
     * @param filterableFields
     * @return
     */
    public static boolean shouldPushDown(ResolvedExpression expr, Set<String> filterableFields) {
        if (expr instanceof CallExpression && expr.getChildren().size() == 2) {
            // 判断是否支持该运算符下推
            return shouldPushDownFunctionDefinition(((CallExpression) expr).getFunctionDefinition())
                    // 判断该字段是否支持下推
                    && shouldPushDownUnaryExpression(expr.getResolvedChildren().get(0), filterableFields)
                    // 判断该字段选定的条件是否支持下推
                    && shouldPushDownUnaryExpression(expr.getResolvedChildren().get(1), filterableFields);
        }
        return false;
    }

    private static boolean shouldPushDownFunctionDefinition(FunctionDefinition functionDefinition) {
        return convert(functionDefinition) != null;
    }

    private static boolean shouldPushDownUnaryExpression(ResolvedExpression expr, Set<String> filterableFields) {
        // validate that type is comparable
        if (!isComparable(expr.getOutputDataType().getConversionClass())) {
            return false;
        }
        if (expr instanceof FieldReferenceExpression) {
            if (filterableFields.contains(((FieldReferenceExpression) expr).getName())) {
                return true;
            }
        }

        if (expr instanceof ValueLiteralExpression) {
            return true;
        }

        if (expr instanceof CallExpression && expr.getChildren().size() == 1) {
            if (((CallExpression) expr).getFunctionDefinition().equals(UPPER)
                    || ((CallExpression) expr).getFunctionDefinition().equals(LOWER)) {
                return shouldPushDownUnaryExpression(
                        expr.getResolvedChildren().get(0), filterableFields);
            }
        }
        // other resolved expressions return false
        return false;
    }

    private static boolean isComparable(Class<?> clazz) {
        return Comparable.class.isAssignableFrom(clazz);
    }
}

