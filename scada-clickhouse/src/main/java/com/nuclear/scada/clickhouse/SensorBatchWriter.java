package com.nuclear.scada.clickhouse;

import com.nuclear.scada.common.model.SensorData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class SensorBatchWriter {

    private final JdbcTemplate clickhouseJdbcTemplate;
    private final ExecutorService writeExecutor;
    private final AtomicLong writtenCount = new AtomicLong(0);

    private static final String INSERT_SQL =
            "INSERT INTO scada_sensor_timeseries " +
            "(plc_id, node_id, sensor_type, value, unit, event_time, quality_code, ingested_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    public SensorBatchWriter(JdbcTemplate clickhouseJdbcTemplate) {
        this.clickhouseJdbcTemplate = clickhouseJdbcTemplate;
        this.writeExecutor = Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "clickhouse-batch-writer");
            t.setDaemon(true);
            return t;
        });
    }

    public void writeBatch(List<SensorData> batch) {
        if (batch == null || batch.isEmpty()) return;

        CompletableFuture.runAsync(() -> {
            try {
                Instant ingestTime = Instant.now();
                clickhouseJdbcTemplate.batchUpdate(INSERT_SQL, batch, batch.size(),
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
                writtenCount.addAndGet(batch.size());
                log.debug("Batch written to ClickHouse: {} records", batch.size());
            } catch (Exception e) {
                log.error("ClickHouse batch write failed, size={}", batch.size(), e);
                throw new RuntimeException(e);
            }
        }, writeExecutor);
    }

    public long getWrittenCount() {
        return writtenCount.get();
    }
}
