package com.lu.table.connector.clickhouse;

import com.aliyun.flink.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
import com.aliyun.flink.connector.clickhouse.table.internal.executor.ClickHouseExecutor;
import com.aliyun.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.sql.SQLException;

public class ClickHouseBatchOutputFormat extends AbstractClickHouseOutputFormat {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseBatchOutputFormat.class);
    private final ClickHouseConnectionProvider connectionProvider;
    private transient ClickHouseConnection connection;
    private final ClickHouseExecutor executor;
    private final ClickHouseOptions options;
    private transient boolean closed = false;
    private transient int batchCount = 0;

    protected ClickHouseBatchOutputFormat(@Nonnull ClickHouseConnectionProvider connectionProvider,
                                          @Nonnull ClickHouseExecutor executor, @Nonnull ClickHouseOptions options) {
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
        this.executor = Preconditions.checkNotNull(executor);
        this.options = Preconditions.checkNotNull(options);
    }

    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            this.connection = this.connectionProvider.getConnection();
            this.executor.prepareStatement(this.connectionProvider);
            this.executor.setRuntimeContext(this.getRuntimeContext());
        } catch (Exception e) {
            throw new IOException("unable to establish connection with ClickHouse", e);
        }
    }

    public void writeRecord(RowData record) throws IOException {
        this.addBatch(record);
        ++this.batchCount;
        if (this.batchCount >= this.options.getBatchSize()) {
            this.flush();
        }

    }

    private void addBatch(RowData record) throws IOException {
        this.executor.addBatch(record);
    }

    public void flush() throws IOException {
        this.executor.executeBatch();
    }

    public void close() throws IOException {
        if (!this.closed) {
            this.closed = true;

            try {
                this.flush();
            } catch (Exception e) {
                LOG.warn("Writing records to ClickHouse failed.", e);
            }

            this.closeConnection();
        }

    }

    private void closeConnection() {
        if (this.connection != null) {
            try {
                this.executor.closeStatement();
                this.connectionProvider.closeConnections();
            } catch (SQLException sqlException) {
                LOG.warn("ClickHouse connection could not be closed: {}", sqlException.getMessage());
            } finally {
                this.connection = null;
            }
        }

    }
}