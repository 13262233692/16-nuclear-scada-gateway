package com.nuclear.scada.clickhouse;

import com.nuclear.scada.common.model.SensorData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class SensorBatchWriter {

    private final JdbcTemplate clickhouseJdbcTemplate;
    private final ExecutorService asyncWriteExecutor;
    private final AtomicLong writtenCount = new AtomicLong(0);
    private final AtomicLong failedCount = new AtomicLong(0);

    private static final String INSERT_SQL =
            "INSERT INTO scada_sensor_timeseries " +
            "(plc_id, node_id, sensor_type, value, unit, event_time, quality_code, ingested_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String DEDUP_INSERT_SQL =
            "INSERT INTO scada_sensor_timeseries " +
            "(plc_id, node_id, sensor_type, value, unit, event_time, quality_code, ingested_at) " +
            "SELECT ?, ?, ?, ?, ?, ?, ?, ? " +
            "WHERE NOT EXISTS (" +
            "  SELECT 1 FROM scada_sensor_timeseries " +
            "  WHERE plc_id = ? AND node_id = ? AND sensor_type = ? AND event_time = ?" +
            ")";

    public SensorBatchWriter(JdbcTemplate clickhouseJdbcTemplate) {
        this.clickhouseJdbcTemplate = clickhouseJdbcTemplate;
        this.asyncWriteExecutor = Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "clickhouse-async-writer");
            t.setDaemon(true);
            return t;
        });
    }

    public void writeBatchSync(List<SensorData> batch) {
        if (batch == null || batch.isEmpty()) return;

        Instant ingestTime = Instant.now();
        int[] results = clickhouseJdbcTemplate.batchUpdate(INSERT_SQL, batch, batch.size(),
                (ps, data) -> {
                    ps.setString(1, data.getPlcId());
                    ps.setString(2, data.getNodeId());
                    ps.setString(3, data.getSensorType().name());
                    ps.setDouble(4, data.getValue());
                    ps.setString(5, data.getUnit());
                    ps.setTimestamp(6, Timestamp.from(data.getTimestamp()));
                    ps.setInt(7, data.getQualityCode().ordinal());
                    ps.setTimestamp(8, Timestamp.from(ingestTime));
                });

        int totalWritten = 0;
        for (int r : results) {
            if (r > 0) totalWritten += r;
        }
        writtenCount.addAndGet(batch.size());
        log.debug("Sync batch written to ClickHouse: {} records", batch.size());
    }

    public void writeBatchSyncWithDedup(List<SensorData> batch) {
        if (batch == null || batch.isEmpty()) return;

        Instant ingestTime = Instant.now();
        clickhouseJdbcTemplate.batchUpdate(DEDUP_INSERT_SQL, batch, batch.size(),
                (ps, data) -> {
                    ps.setString(1, data.getPlcId());
                    ps.setString(2, data.getNodeId());
                    ps.setString(3, data.getSensorType().name());
                    ps.setDouble(4, data.getValue());
                    ps.setString(5, data.getUnit());
                    ps.setTimestamp(6, Timestamp.from(data.getTimestamp()));
                    ps.setInt(7, data.getQualityCode().ordinal());
                    ps.setTimestamp(8, Timestamp.from(ingestTime));

                    ps.setString(9, data.getPlcId());
                    ps.setString(10, data.getNodeId());
                    ps.setString(11, data.getSensorType().name());
                    ps.setTimestamp(12, Timestamp.from(data.getTimestamp()));
                });

        writtenCount.addAndGet(batch.size());
        log.debug("Dedup sync batch written to ClickHouse: {} records", batch.size());
    }

    public void writeBatchAsync(List<SensorData> batch) {
        if (batch == null || batch.isEmpty()) return;

        asyncWriteExecutor.submit(() -> {
            try {
                writeBatchSync(batch);
            } catch (Exception e) {
                failedCount.incrementAndGet();
                log.error("Async ClickHouse batch write failed, size={}", batch.size(), e);
            }
        });
    }

    public long getWrittenCount() {
        return writtenCount.get();
    }

    public long getFailedCount() {
        return failedCount.get();
    }

    public void shutdown() {
        asyncWriteExecutor.shutdown();
        try {
            if (!asyncWriteExecutor.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)) {
                asyncWriteExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            asyncWriteExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
