package com.lu.table.connector.custom.jdbc;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSink;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import java.util.*;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Factory for creating configured instances of {@link JdbcDynamicTableSource}
 * and {@link JdbcDynamicTableSink}.
 */
@Internal
public class CustomJdbcDynamicTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "custom-jdbc";
    public static final ConfigOption<String> URL = ConfigOptions
            .key("url")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc database url.");
    public static final ConfigOption<String> TABLE_NAME = ConfigOptions
            .key("table-name")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc table name.");
    public static final ConfigOption<String> USERNAME = ConfigOptions
            .key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc user name.");
    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc password.");
    private static final ConfigOption<String> DRIVER = ConfigOptions
            .key("driver")
            .stringType()
            .noDefaultValue()
            .withDescription("the class name of the JDBC driver to use to connect to this URL. " +
                    "If not set, it will automatically be derived from the URL.");

    // read config options
    private static final ConfigOption<String> SCAN_PARTITION_COLUMN = ConfigOptions
            .key("scan.partition.column")
            .stringType()
            .noDefaultValue()
            .withDescription("the column name used for partitioning the input.");
    private static final ConfigOption<String> SCAN_PARTITION_COLUMN_TYPE = ConfigOptions
            .key("scan.partition.column.type")
            .stringType()
            .noDefaultValue()
            .withDescription("the column type used for partitioning the input.");
    private static final ConfigOption<Integer> SCAN_PARTITION_NUM = ConfigOptions
            .key("scan.partition.num")
            .intType()
            .noDefaultValue()
            .withDescription("the number of partitions.");
    private static final ConfigOption<String> SCAN_PARTITION_LOWER_BOUND = ConfigOptions
            .key("scan.partition.lower-bound")
            .stringType()
            .noDefaultValue()
            .withDescription("the smallest value of the first partition.");
    private static final ConfigOption<String> SCAN_PARTITION_UPPER_BOUND = ConfigOptions
            .key("scan.partition.upper-bound")
            .stringType()
            .noDefaultValue()
            .withDescription("the largest value of the last partition.");
    private static final ConfigOption<Integer> SCAN_FETCH_SIZE = ConfigOptions
            .key("scan.fetch-size")
            .intType()
            .defaultValue(0)
            .withDescription("gives the reader a hint as to the number of rows that should be fetched, from" +
                    " the database when reading per round trip. If the value specified is zero, then the hint is ignored. The" +
                    " default value is zero.");
    private static final ConfigOption<Boolean> SCAN_AUTO_COMMIT = ConfigOptions
            .key("scan.auto-commit")
            .booleanType()
            .defaultValue(true)
            .withDescription("sets whether the driver is in auto-commit mode. The default value is true, per" +
                    " the JDBC spec.");
    private static final ConfigOption<List<String>> FILTERABLE_FIELDS =
            ConfigOptions.key("filterable-fields").stringType().asList().noDefaultValue();
    private TableSchema physicalSchema;

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        helper.validate();
        validateConfigOptions(config);
        physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        Optional<List<String>> filterableFields = config.getOptional(FILTERABLE_FIELDS);
        Set<String> filterableFieldsSet = new HashSet<>();
        filterableFields.ifPresent(filterableFieldsSet::addAll);

        return new CustomJdbcDynamicTableSource(
                getJdbcOptions(helper.getOptions()),
                getJdbcReadOptions(helper.getOptions()),
                Collections.emptyList(),
                filterableFieldsSet,
                physicalSchema);
    }

    private JdbcOptions getJdbcOptions(ReadableConfig readableConfig) {
        final String url = readableConfig.get(URL);
        final JdbcOptions.Builder builder = JdbcOptions.builder()
                .setDBUrl(url)
                .setTableName(readableConfig.get(TABLE_NAME))
                .setDialect(JdbcDialects.get(url).get());

        readableConfig.getOptional(DRIVER).ifPresent(builder::setDriverName);
        readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
        readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
        return builder.build();
    }

    private CustomJdbcReadOptions getJdbcReadOptions(ReadableConfig readableConfig) {
        final Optional<String> partitionColumnName = readableConfig.getOptional(SCAN_PARTITION_COLUMN);
        final CustomJdbcReadOptions.Builder builder = CustomJdbcReadOptions.builder();
        if (partitionColumnName.isPresent()) {
            builder.setPartitionColumnName(partitionColumnName.get());

            Class<?> conversionClass = physicalSchema.getFieldDataType(partitionColumnName.get())
                    .get().getConversionClass();
            if (!PartitionColumnHelper.checkType(conversionClass)) {
                throw new IllegalArgumentException(String.format(
                        "partition column doesn't support this type, please check it. now only support %s",
                        PartitionColumnHelper.getValues()));
            }

            builder.setPartitionColumnClass(conversionClass);
            builder.setPartitionLowerBound(readableConfig.get(SCAN_PARTITION_LOWER_BOUND));
            builder.setPartitionUpperBound(readableConfig.get(SCAN_PARTITION_UPPER_BOUND));
            builder.setNumPartitions(readableConfig.get(SCAN_PARTITION_NUM));
        }
        readableConfig.getOptional(SCAN_FETCH_SIZE).ifPresent(builder::setFetchSize);
        builder.setAutoCommit(readableConfig.get(SCAN_AUTO_COMMIT));
        return builder.build();
    }

    private JdbcDmlOptions getJdbcDmlOptions(JdbcOptions jdbcOptions, TableSchema schema) {
        String[] keyFields = schema.getPrimaryKey()
                .map(pk -> pk.getColumns().toArray(new String[0]))
                .orElse(null);

        return JdbcDmlOptions.builder()
                .withTableName(jdbcOptions.getTableName())
                .withDialect(jdbcOptions.getDialect())
                .withFieldNames(schema.getFieldNames())
                .withKeyFields(keyFields)
                .build();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(TABLE_NAME);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(DRIVER);
        optionalOptions.add(USERNAME);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(SCAN_PARTITION_COLUMN);
        optionalOptions.add(SCAN_PARTITION_LOWER_BOUND);
        optionalOptions.add(SCAN_PARTITION_UPPER_BOUND);
        optionalOptions.add(SCAN_PARTITION_NUM);
        optionalOptions.add(SCAN_FETCH_SIZE);
        optionalOptions.add(SCAN_AUTO_COMMIT);
        optionalOptions.add(FILTERABLE_FIELDS);
        return optionalOptions;
    }

    private void validateConfigOptions(ReadableConfig config) {
        String jdbcUrl = config.get(URL);
        final Optional<JdbcDialect> dialect = JdbcDialects.get(jdbcUrl);
        checkState(dialect.isPresent(), "Cannot handle such jdbc url: " + jdbcUrl);

        checkAllOrNone(config, new ConfigOption[]{
                USERNAME,
                PASSWORD
        });

        checkAllOrNone(config, new ConfigOption[]{
                SCAN_PARTITION_COLUMN,
                SCAN_PARTITION_NUM,
                SCAN_PARTITION_LOWER_BOUND,
                SCAN_PARTITION_UPPER_BOUND
        });

        if (config.getOptional(SCAN_PARTITION_LOWER_BOUND).isPresent() &&
                config.getOptional(SCAN_PARTITION_UPPER_BOUND).isPresent()) {
            String lowerBound = config.get(SCAN_PARTITION_LOWER_BOUND);
            String upperBound = config.get(SCAN_PARTITION_UPPER_BOUND);
            if (lowerBound.compareTo(upperBound) > 0) {
                throw new IllegalArgumentException(String.format(
                        "'%s'='%s' must not be larger than '%s'='%s'.",
                        SCAN_PARTITION_LOWER_BOUND.key(), lowerBound,
                        SCAN_PARTITION_UPPER_BOUND.key(), upperBound));
            }
        }
    }

    private void checkAllOrNone(ReadableConfig config, ConfigOption<?>[] configOptions) {
        int presentCount = 0;
        for (ConfigOption configOption : configOptions) {
            if (config.getOptional(configOption).isPresent()) {
                presentCount++;
            }
        }
        String[] propertyNames = Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
        Preconditions.checkArgument(configOptions.length == presentCount || presentCount == 0,
                "Either all or none of the following options should be provided:\n" + String.join("\n", propertyNames));
    }
}