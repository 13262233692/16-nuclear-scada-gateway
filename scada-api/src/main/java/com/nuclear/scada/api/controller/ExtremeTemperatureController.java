package com.nuclear.scada.api.controller;

import com.nuclear.scada.clickhouse.SensorQueryRepository;
import com.nuclear.scada.common.model.ExtremeTemperatureReading;
import com.nuclear.scada.common.model.SensorData;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/extreme-temperature")
@RequiredArgsConstructor
public class ExtremeTemperatureController {

    private final SensorQueryRepository queryRepository;

    @GetMapping("/latest")
    public ResponseEntity<List<ExtremeTemperatureReading>> getLatest(
            @RequestParam String plcId,
            @RequestParam(defaultValue = "100") int limit) {
        List<SensorData> data = queryRepository.queryLatest(plcId, "EXTREME_TEMPERATURE", limit);
        List<ExtremeTemperatureReading> readings = data.stream()
                .map(sd -> new ExtremeTemperatureReading(
                        sd.getPlcId(), sd.getNodeId(), sd.getValue(),
                        sd.getNodeId(), sd.getTimestamp(), sd.getQualityCode()))
                .collect(Collectors.toList());
        return ResponseEntity.ok(readings);
    }

    @GetMapping("/range")
    public ResponseEntity<List<ExtremeTemperatureReading>> getByTimeRange(
            @RequestParam String plcId,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant end) {
        List<SensorData> data = queryRepository.queryByTimeRange(plcId, "EXTREME_TEMPERATURE", start, end);
        List<ExtremeTemperatureReading> readings = data.stream()
                .map(sd -> new ExtremeTemperatureReading(
                        sd.getPlcId(), sd.getNodeId(), sd.getValue(),
                        sd.getNodeId(), sd.getTimestamp(), sd.getQualityCode()))
                .collect(Collectors.toList());
        return ResponseEntity.ok(readings);
    }

    @GetMapping("/all-plcs/latest")
    public ResponseEntity<List<ExtremeTemperatureReading>> getAllPlcsLatest(
            @RequestParam(defaultValue = "500") int limit) {
        List<SensorData> data = queryRepository.queryLatestAllPlcs("EXTREME_TEMPERATURE", limit);
        List<ExtremeTemperatureReading> readings = data.stream()
                .map(sd -> new ExtremeTemperatureReading(
                        sd.getPlcId(), sd.getNodeId(), sd.getValue(),
                        sd.getNodeId(), sd.getTimestamp(), sd.getQualityCode()))
                .collect(Collectors.toList());
        return ResponseEntity.ok(readings);
    }
}
