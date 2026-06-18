package com.nuclear.scada.api.controller;

import com.nuclear.scada.clickhouse.SensorQueryRepository;
import com.nuclear.scada.common.model.CoolantPressureReading;
import com.nuclear.scada.common.model.SensorData;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/coolant-pressure")
@RequiredArgsConstructor
public class CoolantPressureController {

    private final SensorQueryRepository queryRepository;

    @GetMapping("/latest")
    public ResponseEntity<List<CoolantPressureReading>> getLatest(
            @RequestParam String plcId,
            @RequestParam(defaultValue = "100") int limit) {
        List<SensorData> data = queryRepository.queryLatest(plcId, "COOLANT_PRESSURE", limit);
        List<CoolantPressureReading> readings = data.stream()
                .map(sd -> new CoolantPressureReading(
                        sd.getPlcId(), sd.getNodeId(), sd.getValue(),
                        sd.getTimestamp(), sd.getQualityCode()))
                .collect(Collectors.toList());
        return ResponseEntity.ok(readings);
    }

    @GetMapping("/range")
    public ResponseEntity<List<CoolantPressureReading>> getByTimeRange(
            @RequestParam String plcId,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant end) {
        List<SensorData> data = queryRepository.queryByTimeRange(plcId, "COOLANT_PRESSURE", start, end);
        List<CoolantPressureReading> readings = data.stream()
                .map(sd -> new CoolantPressureReading(
                        sd.getPlcId(), sd.getNodeId(), sd.getValue(),
                        sd.getTimestamp(), sd.getQualityCode()))
                .collect(Collectors.toList());
        return ResponseEntity.ok(readings);
    }

    @GetMapping("/all-plcs/latest")
    public ResponseEntity<List<CoolantPressureReading>> getAllPlcsLatest(
            @RequestParam(defaultValue = "500") int limit) {
        List<SensorData> data = queryRepository.queryLatestAllPlcs("COOLANT_PRESSURE", limit);
        List<CoolantPressureReading> readings = data.stream()
                .map(sd -> new CoolantPressureReading(
                        sd.getPlcId(), sd.getNodeId(), sd.getValue(),
                        sd.getTimestamp(), sd.getQualityCode()))
                .collect(Collectors.toList());
        return ResponseEntity.ok(readings);
    }
}
