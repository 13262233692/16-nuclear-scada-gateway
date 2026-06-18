package com.nuclear.scada.faulttracing.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FaultTraceResult {

    private String alarmNodeId;
    private String alarmEquipmentId;
    private String alarmType;
    private double alarmValue;
    private Instant alarmTime;
    private Instant analysisTime;
    private List<FaultPathNode> faultPath;
    private List<CandidateRootCause> rootCauses;
    private double overallConfidence;
    private int searchDepth;
    private int nodesVisited;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FaultPathNode {
        private String equipmentId;
        private String name;
        private EquipmentNode.EquipmentType equipmentType;
        private String location;
        private double anomalyScore;
        private double propagationProbability;
        private double cumulativeProbability;
        private int hopDistance;
        private String plcId;
        private String nodeId;
        private List<SensorReading> recentReadings;
        private boolean isAlarmOrigin;
        private boolean isRootCandidate;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CandidateRootCause {
        private String equipmentId;
        private String name;
        private EquipmentNode.EquipmentType equipmentType;
        private String location;
        private String description;
        private double rootCauseProbability;
        private double anomalyScore;
        private int distanceFromAlarm;
        private List<String> affectedEquipment;
        private String recommendedAction;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SensorReading {
        private String sensorType;
        private double value;
        private String unit;
        private Instant timestamp;
        private boolean isAnomalous;
    }
}
