package com.nuclear.scada.faulttracing.controller;

import com.nuclear.scada.faulttracing.model.EquipmentNode;
import com.nuclear.scada.faulttracing.model.FaultTraceResult;
import com.nuclear.scada.faulttracing.repository.EquipmentRepository;
import com.nuclear.scada.faulttracing.service.FaultPropagationService;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/fault")
@RequiredArgsConstructor
@Slf4j
public class FaultTracingController {

    private final FaultPropagationService propagationService;
    private final EquipmentRepository equipmentRepository;

    @PostMapping("/trace")
    public ResponseEntity<FaultTraceResult> traceFault(@RequestBody TraceRequest request) {
        log.info("Received fault trace request: equipment={}, alarmType={}, value={}",
                request.getEquipmentId(), request.getAlarmType(), request.getAlarmValue());

        if (request.getEquipmentId() == null || request.getEquipmentId().trim().isEmpty()) {
            return ResponseEntity.badRequest().build();
        }

        FaultTraceResult result = propagationService.traceFault(
                request.getEquipmentId().trim(),
                request.getAlarmType() != null ? request.getAlarmType() : "PRESSURE_DROP",
                request.getAlarmValue() != null ? request.getAlarmValue() : 0.0
        );

        return ResponseEntity.ok(result);
    }

    @GetMapping("/trace")
    public ResponseEntity<FaultTraceResult> traceFaultGet(
            @RequestParam String equipmentId,
            @RequestParam(defaultValue = "PRESSURE_DROP") String alarmType,
            @RequestParam(defaultValue = "0.0") double alarmValue) {
        log.info("Received fault trace GET request: equipment={}, alarmType={}, value={}",
                equipmentId, alarmType, alarmValue);

        FaultTraceResult result = propagationService.traceFault(
                equipmentId.trim(),
                alarmType,
                alarmValue
        );

        return ResponseEntity.ok(result);
    }

    @GetMapping("/topology/equipment")
    public ResponseEntity<List<Map<String, Object>>> listEquipment(
            @RequestParam(required = false) String type) {
        List<EquipmentNode> equipment;
        if (type != null && !type.isEmpty()) {
            try {
                equipment = equipmentRepository.findByEquipmentType(
                        EquipmentNode.EquipmentType.valueOf(type.toUpperCase()));
            } catch (IllegalArgumentException e) {
                return ResponseEntity.badRequest().build();
            }
        } else {
            equipment = equipmentRepository.findAll();
        }

        List<Map<String, Object>> result = equipment.stream()
                .map(this::mapToSummary)
                .collect(Collectors.toList());

        return ResponseEntity.ok(result);
    }

    @GetMapping("/topology/equipment/{equipmentId}")
    public ResponseEntity<Map<String, Object>> getEquipment(
            @RequestParam String equipmentId) {
        Optional<EquipmentNode> opt = equipmentRepository.findByEquipmentId(equipmentId);
        if (!opt.isPresent()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(mapToDetail(opt.get()));
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getHealth() {
        long equipmentCount = equipmentRepository.count();
        return ResponseEntity.ok(Map.of(
                "status", "OK",
                "timestamp", Instant.now().toString(),
                "equipmentCount", equipmentCount
        ));
    }

    private Map<String, Object> mapToSummary(EquipmentNode eq) {
        return Map.of(
                "equipmentId", eq.getEquipmentId(),
                "name", eq.getName(),
                "equipmentType", eq.getEquipmentType().name(),
                "location", eq.getLocation() != null ? eq.getLocation() : "",
                "criticality", eq.getCriticality(),
                "status", eq.getStatus() != null ? eq.getStatus() : "UNKNOWN"
        );
    }

    private Map<String, Object> mapToDetail(EquipmentNode eq) {
        return Map.of(
                "equipmentId", eq.getEquipmentId(),
                "name", eq.getName(),
                "equipmentType", eq.getEquipmentType().name(),
                "location", eq.getLocation() != null ? eq.getLocation() : "",
                "plcId", eq.getPlcId() != null ? eq.getPlcId() : "",
                "nodeId", eq.getNodeId() != null ? eq.getNodeId() : "",
                "criticality", eq.getCriticality(),
                "status", eq.getStatus() != null ? eq.getStatus() : "UNKNOWN",
                "incomingConnections", eq.getIncomingConnections().stream()
                        .map(c -> Map.of(
                                "from", c.getTarget().getEquipmentId(),
                                "propagationProbability", c.getPropagationProbability(),
                                "flowDirection", c.getFlowDirection().name()
                        ))
                        .collect(Collectors.toList()),
                "outgoingConnections", eq.getOutgoingConnections().stream()
                        .map(c -> Map.of(
                                "to", c.getTarget().getEquipmentId(),
                                "propagationProbability", c.getPropagationProbability(),
                                "flowDirection", c.getFlowDirection().name()
                        ))
                        .collect(Collectors.toList())
        );
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TraceRequest {
        private String equipmentId;
        private String alarmType;
        private Double alarmValue;
        private String alarmDescription;
        @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
        private Instant alarmTime;
    }
}
