package com.nuclear.scada.clickhouse;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class ClickHouseSchemaInitializer {

    private final JdbcTemplate clickhouseJdbcTemplate;

    private static final String CREATE_DATABASE_SQL = "CREATE DATABASE IF NOT EXISTS scada";

    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS scada.scada_sensor_timeseries (" +
            "  plc_id String," +
            "  node_id String," +
            "  sensor_type String," +
            "  value Float64," +
            "  unit String," +
            "  event_time DateTime64(3)," +
            "  quality_code UInt8," +
            "  ingested_at DateTime64(3)" +
            ") " +
            "ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(event_time) " +
            "ORDER BY (plc_id, sensor_type, node_id, event_time) " +
            "TTL event_time + INTERVAL 90 DAY " +
            "SETTINGS index_granularity = 8192";

    private static final String CREATE_MV_LATEST_SQL =
            "CREATE MATERIALIZED VIEW IF NOT EXISTS scada.mv_latest_sensor " +
            "ENGINE = AggregatingMergeTree() " +
            "PARTITION BY plc_id " +
            "ORDER BY (plc_id, sensor_type, node_id) " +
            "AS SELECT " +
            "  plc_id, " +
            "  sensor_type, " +
            "  node_id, " +
            "  argMaxState(value, event_time) AS latest_value, " +
            "  argMaxState(event_time, event_time) AS latest_time, " +
            "  argMaxState(quality_code, event_time) AS latest_quality " +
            "FROM scada.scada_sensor_timeseries " +
            "GROUP BY plc_id, sensor_type, node_id";

    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        try {
            clickhouseJdbcTemplate.execute(CREATE_DATABASE_SQL);
            clickhouseJdbcTemplate.execute(CREATE_TABLE_SQL);
            clickhouseJdbcTemplate.execute(CREATE_MV_LATEST_SQL);
            log.info("ClickHouse schema initialized successfully");
        } catch (Exception e) {
            log.warn("ClickHouse schema init failed (may already exist): {}", e.getMessage());
        }
    }
}
