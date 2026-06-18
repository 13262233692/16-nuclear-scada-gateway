package com.nuclear.scada.api.controller;

import com.nuclear.scada.clickhouse.SensorQueryRepository;
import com.nuclear.scada.common.model.SensorData;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/api/v1/sensor")
@RequiredArgsConstructor
public class SensorDataController {

    private final SensorQueryRepository queryRepository;

    @GetMapping("/latest")
    public ResponseEntity<List<SensorData>> getLatest(
            @RequestParam String plcId,
            @RequestParam String sensorType,
            @RequestParam(defaultValue = "100") int limit) {
        List<SensorData> data = queryRepository.queryLatest(plcId, sensorType, limit);
        return ResponseEntity.ok(data);
    }

    @GetMapping("/range")
    public ResponseEntity<List<SensorData>> getByTimeRange(
            @RequestParam String plcId,
            @RequestParam String sensorType,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant end) {
        List<SensorData> data = queryRepository.queryByTimeRange(plcId, sensorType, start, end);
        return ResponseEntity.ok(data);
    }

    @GetMapping("/latest-all")
    public ResponseEntity<List<SensorData>> getLatestAll(
            @RequestParam String sensorType,
            @RequestParam(defaultValue = "500") int limit) {
        List<SensorData> data = queryRepository.queryLatestAllPlcs(sensorType, limit);
        return ResponseEntity.ok(data);
    }

    @GetMapping("/overview")
    public ResponseEntity<List<SensorQueryRepository.PlcOverview>> getOverview() {
        return ResponseEntity.ok(queryRepository.getPlcOverview());
    }
}
