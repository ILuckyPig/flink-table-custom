package com.lu.table.connector;

import com.lu.table.factories.ConcurrencyReadFactoryUtil;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.*;

import static com.lu.table.connector.JdbcBatchValidator.*;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.EXPR;
import static org.apache.flink.table.descriptors.Schema.*;

public class JdbcBatchTableSourceFactory implements BatchTableSourceFactory<Row> {
    @Override
    public BatchTableSource<Row> createBatchTableSource(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA));

        return JdbcBatchTableSource.builder()
                .setOptions(getJdbcOptions(descriptorProperties))
                .setReadOptions(getJdbcReadOptions(descriptorProperties))
                .setConcurrencyOptions(getJdbcColumnConcurrencyOptions(descriptorProperties))
                .setSchema(schema)
                .build();
    }

    @Override
    public Map<String, String> requiredContext() {
        final Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_JDBC);
        context.put(CONNECTOR_PROPERTY_VERSION, "1");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();
        // common options
        properties.add(CONNECTOR_DRIVER);
        properties.add(CONNECTOR_URL);
        properties.add(CONNECTOR_TABLE);
        properties.add(CONNECTOR_USERNAME);
        properties.add(CONNECTOR_PASSWORD);

        // scan options
        properties.add(CONNECTOR_READ_QUERY);
        properties.add(CONNECTOR_READ_PARTITION_COLUMN);
        properties.add(CONNECTOR_READ_PARTITION_NUM);
        properties.add(CONNECTOR_READ_PARTITION_LOWER_BOUND);
        properties.add(CONNECTOR_READ_PARTITION_UPPER_BOUND);
        properties.add(CONNECTOR_READ_FETCH_SIZE);


        properties.add(CONNECTOR_READ_PARTITION_TYPE);
        properties.add(CONNECTOR_READ_PARTITION_COLUMN_TYPE);

        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);
        // computed column
        properties.add(SCHEMA + ".#." + EXPR);

        return properties;
    }

    private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        new SchemaValidator(false, false, false)
                .validate(descriptorProperties);
        // new JdbcBatchValidator().validate(descriptorProperties);
        // TODO 验证并发参数

        return descriptorProperties;
    }

    private JdbcOptions getJdbcOptions(DescriptorProperties descriptorProperties) {
        final String url = descriptorProperties.getString(CONNECTOR_URL);
        final JdbcOptions.Builder builder = JdbcOptions.builder()
                .setDBUrl(url)
                .setTableName(descriptorProperties.getString(CONNECTOR_TABLE))
                .setDialect(JdbcDialects.get(url).get());

        descriptorProperties.getOptionalString(CONNECTOR_DRIVER).ifPresent(builder::setDriverName);
        descriptorProperties.getOptionalString(CONNECTOR_USERNAME).ifPresent(builder::setUsername);
        descriptorProperties.getOptionalString(CONNECTOR_PASSWORD).ifPresent(builder::setPassword);

        return builder.build();
    }

    private JdbcReadOptions getJdbcReadOptions(DescriptorProperties descriptorProperties) {
        final Optional<String> query = descriptorProperties.getOptionalString(CONNECTOR_READ_QUERY);
        final Optional<String> partitionColumnName = descriptorProperties.getOptionalString(CONNECTOR_READ_PARTITION_COLUMN);
        final Optional<String> partitionColumnType = descriptorProperties.getOptionalString(CONNECTOR_READ_PARTITION_COLUMN_TYPE);

        final JdbcReadOptions.Builder builder = JdbcReadOptions.builder();
        query.ifPresent(builder::setQuery);
        if (partitionColumnName.isPresent() && !partitionColumnType.isPresent()) {
            final Optional<Long> partitionLower = descriptorProperties.getOptionalLong(CONNECTOR_READ_PARTITION_LOWER_BOUND);
            final Optional<Long> partitionUpper = descriptorProperties.getOptionalLong(CONNECTOR_READ_PARTITION_UPPER_BOUND);
            final Optional<Integer> numPartitions = descriptorProperties.getOptionalInt(CONNECTOR_READ_PARTITION_NUM);
            builder.setPartitionColumnName(partitionColumnName.get());
            builder.setPartitionLowerBound(partitionLower.get());
            builder.setPartitionUpperBound(partitionUpper.get());
            builder.setNumPartitions(numPartitions.get());
        }
        descriptorProperties.getOptionalInt(CONNECTOR_READ_FETCH_SIZE).ifPresent(builder::setFetchSize);

        return builder.build();
    }

    private JdbcColumnConcurrencyOptions getJdbcColumnConcurrencyOptions(DescriptorProperties descriptorProperties) {
        final Optional<String> partitionColumnName = descriptorProperties.getOptionalString(CONNECTOR_READ_PARTITION_COLUMN);
        final Optional<String> partitionColumnType = descriptorProperties.getOptionalString(CONNECTOR_READ_PARTITION_COLUMN_TYPE);

        final JdbcColumnConcurrencyOptions.Builder builder = JdbcColumnConcurrencyOptions.builder();
        if (partitionColumnName.isPresent() && partitionColumnType.isPresent()) {
            List<Object> concurrencyReadList = ConcurrencyReadFactoryUtil.findAndCreateConcurrencyReadList(descriptorProperties.asMap());
            builder.setPartitionColumnName(partitionColumnName.get());
            builder.setPartitionColumnType(partitionColumnType.get());
            builder.setConcurrencyReadList(concurrencyReadList);
        }
        return builder.build();
    }
}
