package com.nuclear.scada.clickhouse;

import com.nuclear.scada.common.model.SensorData;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
@RequiredArgsConstructor
@Slf4j
public class SensorQueryRepository {

    private final JdbcTemplate clickhouseJdbcTemplate;

    public List<SensorData> queryLatest(String plcId, String sensorType, int limit) {
        String sql = "SELECT plc_id, node_id, sensor_type, value, unit, event_time, quality_code " +
                "FROM scada_sensor_timeseries " +
                "WHERE plc_id = ? AND sensor_type = ? " +
                "ORDER BY event_time DESC LIMIT ?";
        return clickhouseJdbcTemplate.query(sql,
                (rs, rowNum) -> SensorData.builder()
                        .plcId(rs.getString("plc_id"))
                        .nodeId(rs.getString("node_id"))
                        .sensorType(SensorData.SensorType.valueOf(rs.getString("sensor_type")))
                        .value(rs.getDouble("value"))
                        .unit(rs.getString("unit"))
                        .timestamp(rs.getTimestamp("event_time").toInstant())
                        .qualityCode(SensorData.QualityCode.fromCode(rs.getInt("quality_code")))
                        .build(),
                plcId, sensorType, limit);
    }

    public List<SensorData> queryByTimeRange(String plcId, String sensorType,
                                              Instant start, Instant end) {
        String sql = "SELECT plc_id, node_id, sensor_type, value, unit, event_time, quality_code " +
                "FROM scada_sensor_timeseries " +
                "WHERE plc_id = ? AND sensor_type = ? " +
                "AND event_time >= ? AND event_time <= ? " +
                "ORDER BY event_time ASC";
        return clickhouseJdbcTemplate.query(sql,
                (rs, rowNum) -> SensorData.builder()
                        .plcId(rs.getString("plc_id"))
                        .nodeId(rs.getString("node_id"))
                        .sensorType(SensorData.SensorType.valueOf(rs.getString("sensor_type")))
                        .value(rs.getDouble("value"))
                        .unit(rs.getString("unit"))
                        .timestamp(rs.getTimestamp("event_time").toInstant())
                        .qualityCode(SensorData.QualityCode.fromCode(rs.getInt("quality_code")))
                        .build(),
                plcId, sensorType, start, end);
    }

    public List<SensorData> queryLatestAllPlcs(String sensorType, int limit) {
        String sql = "SELECT plc_id, node_id, sensor_type, value, unit, event_time, quality_code " +
                "FROM scada_sensor_timeseries " +
                "WHERE sensor_type = ? " +
                "ORDER BY event_time DESC LIMIT ?";
        return clickhouseJdbcTemplate.query(sql,
                (rs, rowNum) -> SensorData.builder()
                        .plcId(rs.getString("plc_id"))
                        .nodeId(rs.getString("node_id"))
                        .sensorType(SensorData.SensorType.valueOf(rs.getString("sensor_type")))
                        .value(rs.getDouble("value"))
                        .unit(rs.getString("unit"))
                        .timestamp(rs.getTimestamp("event_time").toInstant())
                        .qualityCode(SensorData.QualityCode.fromCode(rs.getInt("quality_code")))
                        .build(),
                sensorType, limit);
    }

    public List<PlcOverview> getPlcOverview() {
        String sql = "SELECT plc_id, argMax(value, event_time) AS latest_value, " +
                "max(event_time) AS last_event_time, count() AS reading_count " +
                "FROM scada_sensor_timeseries GROUP BY plc_id ORDER BY plc_id";
        return clickhouseJdbcTemplate.query(sql,
                (rs, rowNum) -> new PlcOverview(
                        rs.getString("plc_id"),
                        rs.getDouble("latest_value"),
                        rs.getTimestamp("last_event_time").toInstant(),
                        rs.getLong("reading_count")
                ));
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PlcOverview {
        private String plcId;
        private double latestValue;
        private Instant lastEventTime;
        private long readingCount;
    }
}
