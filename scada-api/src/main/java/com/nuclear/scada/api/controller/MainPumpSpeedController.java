package com.nuclear.scada.api.controller;

import com.nuclear.scada.clickhouse.SensorQueryRepository;
import com.nuclear.scada.common.model.MainPumpSpeedReading;
import com.nuclear.scada.common.model.SensorData;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/pump-speed")
@RequiredArgsConstructor
public class MainPumpSpeedController {

    private final SensorQueryRepository queryRepository;

    @GetMapping("/latest")
    public ResponseEntity<List<MainPumpSpeedReading>> getLatest(
            @RequestParam String plcId,
            @RequestParam(defaultValue = "100") int limit) {
        List<SensorData> data = queryRepository.queryLatest(plcId, "MAIN_PUMP_SPEED", limit);
        List<MainPumpSpeedReading> readings = data.stream()
                .map(sd -> new MainPumpSpeedReading(
                        sd.getPlcId(), sd.getNodeId(), sd.getValue(),
                        sd.getTimestamp(), sd.getQualityCode()))
                .collect(Collectors.toList());
        return ResponseEntity.ok(readings);
    }

    @GetMapping("/range")
    public ResponseEntity<List<MainPumpSpeedReading>> getByTimeRange(
            @RequestParam String plcId,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant end) {
        List<SensorData> data = queryRepository.queryByTimeRange(plcId, "MAIN_PUMP_SPEED", start, end);
        List<MainPumpSpeedReading> readings = data.stream()
                .map(sd -> new MainPumpSpeedReading(
                        sd.getPlcId(), sd.getNodeId(), sd.getValue(),
                        sd.getTimestamp(), sd.getQualityCode()))
                .collect(Collectors.toList());
        return ResponseEntity.ok(readings);
    }

    @GetMapping("/all-plcs/latest")
    public ResponseEntity<List<MainPumpSpeedReading>> getAllPlcsLatest(
            @RequestParam(defaultValue = "500") int limit) {
        List<SensorData> data = queryRepository.queryLatestAllPlcs("MAIN_PUMP_SPEED", limit);
        List<MainPumpSpeedReading> readings = data.stream()
                .map(sd -> new MainPumpSpeedReading(
                        sd.getPlcId(), sd.getNodeId(), sd.getValue(),
                        sd.getTimestamp(), sd.getQualityCode()))
                .collect(Collectors.toList());
        return ResponseEntity.ok(readings);
    }
}
