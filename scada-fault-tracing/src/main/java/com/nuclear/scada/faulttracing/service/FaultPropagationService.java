package com.nuclear.scada.faulttracing.service;

import com.nuclear.scada.common.model.SensorData;
import com.nuclear.scada.clickhouse.SensorQueryRepository;
import com.nuclear.scada.faulttracing.model.AnomalyScore;
import com.nuclear.scada.faulttracing.model.Connection;
import com.nuclear.scada.faulttracing.model.EquipmentNode;
import com.nuclear.scada.faulttracing.model.FaultTraceResult;
import com.nuclear.scada.faulttracing.repository.EquipmentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class FaultPropagationService {

    private final EquipmentRepository equipmentRepository;
    private final AnomalyDetectionService anomalyDetectionService;
    private final SensorQueryRepository sensorRepository;
    private final Driver neo4jDriver;

    @Value("${scada.fault-tracing.max-search-depth:6}")
    private int maxSearchDepth;

    @Value("${scada.fault-tracing.anomaly-time-window-minutes:5}")
    private int anomalyTimeWindowMinutes;

    @Value("${scada.fault-tracing.min-root-cause-probability:0.3}")
    private double minRootCauseProbability;

    public FaultTraceResult traceFault(String alarmEquipmentId, String alarmType,
                                       double alarmValue) {
        log.info("Starting fault trace for alarm: equipment={}, type={}, value={}",
                alarmEquipmentId, alarmType, alarmValue);

        Optional<EquipmentNode> alarmOpt = equipmentRepository.findByEquipmentId(alarmEquipmentId);
        if (!alarmOpt.isPresent()) {
            throw new IllegalArgumentException("Alarm equipment not found: " + alarmEquipmentId);
        }

        EquipmentNode alarmNode = alarmOpt.get();
        Instant alarmTime = Instant.now();

        Map<String, BfsNode> upstreamGraph = reverseBfsUpstream(alarmNode);

        Map<String, AnomalyScore> anomalyScores = calculateAllAnomalyScores(upstreamGraph.keySet());

        AnomalyScore alarmAnomaly = anomalyScores.getOrDefault(alarmEquipmentId,
                AnomalyScore.critical(alarmEquipmentId, 1.0));
        anomalyScores.put(alarmEquipmentId, alarmAnomaly);

        List<FaultTraceResult.FaultPathNode> faultPath = buildFaultPath(
                alarmNode, upstreamGraph, anomalyScores);

        List<FaultTraceResult.CandidateRootCause> rootCauses = identifyRootCauses(
                upstreamGraph, anomalyScores, alarmNode);

        double overallConfidence = calculateOverallConfidence(rootCauses, faultPath);

        return FaultTraceResult.builder()
                .alarmNodeId(alarmNode.getNodeId())
                .alarmEquipmentId(alarmEquipmentId)
                .alarmType(alarmType)
                .alarmValue(alarmValue)
                .alarmTime(alarmTime)
                .analysisTime(Instant.now())
                .faultPath(faultPath)
                .rootCauses(rootCauses)
                .overallConfidence(overallConfidence)
                .searchDepth(maxSearchDepth)
                .nodesVisited(upstreamGraph.size())
                .build();
    }

    private Map<String, BfsNode> reverseBfsUpstream(EquipmentNode alarmNode) {
        Map<String, BfsNode> upstreamGraph = new HashMap<>();
        Set<String> visited = new HashSet<>();
        Queue<BfsNode> queue = new LinkedList<>();

        BfsNode alarmBfs = new BfsNode(alarmNode, 0, 1.0, null, null);
        queue.add(alarmBfs);
        upstreamGraph.put(alarmNode.getEquipmentId(), alarmBfs);
        visited.add(alarmNode.getEquipmentId());

        try (Session session = neo4jDriver.session()) {
            while (!queue.isEmpty()) {
                BfsNode current = queue.poll();

                if (current.distance >= maxSearchDepth) continue;

                List<Record> upstreamRecords = session.run(
                    "MATCH (current:Equipment {equipment_id: $currentId}) " +
                    "MATCH (upstream:Equipment)-[r:CONNECTS_TO]->(current) " +
                    "WHERE r.flow_direction IN ['FORWARD', 'BIDIRECTIONAL'] " +
                    "RETURN upstream, r",
                    Collections.singletonMap("currentId", current.equipment.getEquipmentId())
                ).list();

                for (Record record : upstreamRecords) {
                    EquipmentNode upstream = mapToEquipment(record.get("upstream"));
                    Connection conn = mapToConnection(record.get("r"));

                    if (upstream == null || visited.contains(upstream.getEquipmentId())) continue;

                    visited.add(upstream.getEquipmentId());

                    double cumulativeProb = current.cumulativePropagationProb
                            * conn.getPropagationProbability()
                            * upstream.getCriticality();

                    BfsNode upstreamBfs = new BfsNode(
                            upstream,
                            current.distance + 1,
                            cumulativeProb,
                            current.equipment.getEquipmentId(),
                            conn
                    );

                    upstreamGraph.put(upstream.getEquipmentId(), upstreamBfs);
                    queue.add(upstreamBfs);
                }
            }
        }

        log.debug("Reverse BFS completed: visited {} upstream nodes from alarm {}",
                upstreamGraph.size(), alarmNode.getEquipmentId());
        return upstreamGraph;
    }

    private Map<String, AnomalyScore> calculateAllAnomalyScores(Set<String> equipmentIds) {
        Map<String, AnomalyScore> scores = new HashMap<>();
        Duration timeWindow = Duration.ofMinutes(anomalyTimeWindowMinutes);

        for (String equipmentId : equipmentIds) {
            try {
                Optional<EquipmentNode> eqOpt = equipmentRepository.findByEquipmentId(equipmentId);
                if (eqOpt.isPresent()) {
                    AnomalyScore score = anomalyDetectionService.calculateAnomalyScore(
                            eqOpt.get(), timeWindow);
                    scores.put(equipmentId, score);
                } else {
                    scores.put(equipmentId, AnomalyScore.normal(equipmentId));
                }
            } catch (Exception e) {
                log.warn("Failed to calculate anomaly score for {}: {}", equipmentId, e.getMessage());
                scores.put(equipmentId, AnomalyScore.normal(equipmentId));
            }
        }
        return scores;
    }

    private List<FaultTraceResult.FaultPathNode> buildFaultPath(
            EquipmentNode alarmNode,
            Map<String, BfsNode> upstreamGraph,
            Map<String, AnomalyScore> anomalyScores) {

        List<FaultTraceResult.FaultPathNode> path = new ArrayList<>();
        Set<String> rootIds = upstreamGraph.values().stream()
                .filter(n -> n.distance == maxSearchDepth || !hasUpstream(n.equipment))
                .map(n -> n.equipment.getEquipmentId())
                .collect(Collectors.toSet());

        for (BfsNode node : upstreamGraph.values()) {
            AnomalyScore anomaly = anomalyScores.getOrDefault(
                    node.equipment.getEquipmentId(),
                    AnomalyScore.normal(node.equipment.getEquipmentId()));

            boolean isAlarm = node.equipment.getEquipmentId().equals(alarmNode.getEquipmentId());
            boolean isRoot = rootIds.contains(node.equipment.getEquipmentId());

            double combinedProbability = node.cumulativePropagationProb
                    * (0.3 + 0.7 * anomaly.getTotalScore());

            FaultTraceResult.FaultPathNode pathNode = FaultTraceResult.FaultPathNode.builder()
                    .equipmentId(node.equipment.getEquipmentId())
                    .name(node.equipment.getName())
                    .equipmentType(node.equipment.getEquipmentType())
                    .location(node.equipment.getLocation())
                    .anomalyScore(anomaly.getTotalScore())
                    .propagationProbability(node.connection != null ?
                            node.connection.getPropagationProbability() : 1.0)
                    .cumulativeProbability(Math.min(1.0, combinedProbability))
                    .hopDistance(node.distance)
                    .plcId(node.equipment.getPlcId())
                    .nodeId(node.equipment.getNodeId())
                    .recentReadings(getRecentReadings(node.equipment))
                    .isAlarmOrigin(isAlarm)
                    .isRootCandidate(isRoot)
                    .build();

            path.add(pathNode);
        }

        path.sort((a, b) -> {
            int distCompare = Integer.compare(a.getHopDistance(), b.getHopDistance());
            if (distCompare != 0) return distCompare;
            return Double.compare(b.getCumulativeProbability(), a.getCumulativeProbability());
        });

        return path;
    }

    private List<FaultTraceResult.CandidateRootCause> identifyRootCauses(
            Map<String, BfsNode> upstreamGraph,
            Map<String, AnomalyScore> anomalyScores,
            EquipmentNode alarmNode) {

        List<FaultTraceResult.CandidateRootCause> candidates = new ArrayList<>();

        for (BfsNode node : upstreamGraph.values()) {
            String equipId = node.equipment.getEquipmentId();
            if (equipId.equals(alarmNode.getEquipmentId())) continue;

            AnomalyScore anomaly = anomalyScores.getOrDefault(equipId,
                    AnomalyScore.normal(equipId));

            if (anomaly.getTotalScore() < 0.15) continue;

            double rootCauseProb = calculateRootCauseProbability(
                    node, anomaly, upstreamGraph, anomalyScores);

            if (rootCauseProb >= minRootCauseProbability) {
                List<String> affected = findAffectedEquipment(node.equipment, upstreamGraph);
                String description = generateRootCauseDescription(node.equipment, anomaly);
                String action = generateRecommendedAction(node.equipment, anomaly);

                FaultTraceResult.CandidateRootCause candidate =
                        FaultTraceResult.CandidateRootCause.builder()
                                .equipmentId(node.equipment.getEquipmentId())
                                .name(node.equipment.getName())
                                .equipmentType(node.equipment.getEquipmentType())
                                .location(node.equipment.getLocation())
                                .description(description)
                                .rootCauseProbability(rootCauseProb)
                                .anomalyScore(anomaly.getTotalScore())
                                .distanceFromAlarm(node.distance)
                                .affectedEquipment(affected)
                                .recommendedAction(action)
                                .build();

                candidates.add(candidate);
            }
        }

        candidates.sort((a, b) ->
                Double.compare(b.getRootCauseProbability(), a.getRootCauseProbability()));

        return candidates.stream().limit(5).collect(Collectors.toList());
    }

    private double calculateRootCauseProbability(
            BfsNode node, AnomalyScore anomaly,
            Map<String, BfsNode> upstreamGraph,
            Map<String, AnomalyScore> anomalyScores) {

        if (anomaly.getTotalScore() < 0.15) return 0.0;

        double distanceFactor = 1.0 - (node.distance * 0.1);
        double propagationFactor = node.cumulativePropagationProb;
        double anomalyFactor = anomaly.getTotalScore();
        double criticalityFactor = node.equipment.getCriticality();

        boolean hasNoUpstreamAnomaly = true;
        for (BfsNode upstreamNode : upstreamGraph.values()) {
            if (upstreamNode.parentId != null
                    && upstreamNode.parentId.equals(node.equipment.getEquipmentId())) {
                AnomalyScore upstreamAnomaly = anomalyScores.getOrDefault(
                        upstreamNode.equipment.getEquipmentId(),
                        AnomalyScore.normal(upstreamNode.equipment.getEquipmentId()));
                if (upstreamAnomaly.getTotalScore() > anomaly.getTotalScore() * 0.8) {
                    hasNoUpstreamAnomaly = false;
                    break;
                }
            }
        }

        double isolationFactor = hasNoUpstreamAnomaly ? 1.3 : 0.7;

        double combined = (anomalyFactor * 0.45
                         + propagationFactor * 0.25
                         + criticalityFactor * 0.20
                         + distanceFactor * 0.10)
                         * isolationFactor;

        return Math.min(1.0, Math.max(0.0, combined));
    }

    private boolean hasUpstream(EquipmentNode node) {
        try (Session session = neo4jDriver.session()) {
            Record record = session.run(
                "MATCH (n:Equipment {equipment_id: $id}) " +
                "MATCH (up:Equipment)-[r:CONNECTS_TO]->(n) " +
                "WHERE r.flow_direction IN ['FORWARD', 'BIDIRECTIONAL'] " +
                "RETURN count(up) as cnt",
                Collections.singletonMap("id", node.getEquipmentId())
            ).single();
            return record.get("cnt").asLong() > 0;
        } catch (Exception e) {
            return false;
        }
    }

    private List<String> findAffectedEquipment(
            EquipmentNode root, Map<String, BfsNode> upstreamGraph) {

        List<String> affected = new ArrayList<>();
        for (BfsNode node : upstreamGraph.values()) {
            String current = node.parentId;
            while (current != null) {
                if (current.equals(root.getEquipmentId())) {
                    affected.add(node.equipment.getName());
                    break;
                }
                BfsNode parentNode = upstreamGraph.get(current);
                current = parentNode != null ? parentNode.parentId : null;
            }
        }
        return affected;
    }

    private List<FaultTraceResult.SensorReading> getRecentReadings(EquipmentNode equipment) {
        List<FaultTraceResult.SensorReading> readings = new ArrayList<>();
        String plcId = equipment.getPlcId();
        if (plcId == null || plcId.isEmpty()) return readings;

        try {
            Instant end = Instant.now();
            Instant start = end.minus(Duration.ofMinutes(5));

            for (String sensorType : new String[] {"COOLANT_PRESSURE", "MAIN_PUMP_SPEED", "EXTREME_TEMPERATURE"}) {
                try {
                    List<SensorData> data = sensorRepository.queryByTimeRange(
                            plcId, sensorType, start, end);
                    if (!data.isEmpty()) {
                        SensorData latest = data.get(data.size() - 1);
                        readings.add(FaultTraceResult.SensorReading.builder()
                                .sensorType(sensorType)
                                .value(latest.getValue())
                                .unit(latest.getUnit())
                                .timestamp(latest.getTimestamp())
                                .isAnomalous(latest.getQualityCode() != SensorData.QualityCode.GOOD)
                                .build());
                    }
                } catch (Exception ignored) {
                }
            }
        } catch (Exception e) {
            log.debug("No recent readings for {}", equipment.getEquipmentId());
        }
        return readings;
    }

    private String generateRootCauseDescription(EquipmentNode eq, AnomalyScore anomaly) {
        StringBuilder sb = new StringBuilder();
        sb.append(eq.getEquipmentType()).append(" '").append(eq.getName()).append("' ");

        if (anomaly.getTotalScore() >= 0.8) {
            sb.append("exhibits severe abnormal behavior");
        } else if (anomaly.getTotalScore() >= 0.5) {
            sb.append("shows significant abnormal characteristics");
        } else {
            sb.append("demonstrates mild abnormal patterns");
        }

        List<String> details = new ArrayList<>();
        if (anomaly.getPressureAnomaly() > 0.5) {
            details.add(String.format("pressure anomaly (%.1f%%)", anomaly.getPressureAnomaly() * 100));
        }
        if (anomaly.getTemperatureAnomaly() > 0.5) {
            details.add(String.format("temperature anomaly (%.1f%%)", anomaly.getTemperatureAnomaly() * 100));
        }
        if (anomaly.getFlowAnomaly() > 0.5) {
            details.add(String.format("flow irregularity (%.1f%%)", anomaly.getFlowAnomaly() * 100));
        }
        if (anomaly.getVibrationAnomaly() > 0.5) {
            details.add(String.format("vibration anomaly (%.1f%%)", anomaly.getVibrationAnomaly() * 100));
        }

        if (!details.isEmpty()) {
            sb.append(": ").append(String.join(", ", details));
        }

        return sb.toString();
    }

    private String generateRecommendedAction(EquipmentNode eq, AnomalyScore anomaly) {
        switch (eq.getEquipmentType()) {
            case VALVE:
                if (anomaly.getFlowAnomaly() > 0.7) {
                    return "紧急检查阀门位置反馈，可能存在卡死或无法调节情况，准备隔离阀切换";
                } else if (anomaly.getPressureAnomaly() > 0.6) {
                    return "检查阀杆密封与执行机构油压，必要时在线切换备用回路";
                }
                return "按规程检查阀门运行状态与历史趋势，安排预防性巡检";

            case PUMP:
                if (anomaly.getVibrationAnomaly() > 0.7 || anomaly.getFlowAnomaly() > 0.7) {
                    return "立即启动备泵，故障泵停机检查轴承与叶轮磨损情况";
                } else if (anomaly.getPressureAnomaly() > 0.5) {
                    return "检查泵出口压力与密封水系统，评估是否需要降功率运行";
                }
                return "监测泵运行噪声与振动频谱，安排振动专家分析";

            case PIPE:
                if (anomaly.getTemperatureAnomaly() > 0.7) {
                    return "检查管道保温完整性，评估热应力裂纹风险";
                } else if (anomaly.getPressureAnomaly() > 0.6) {
                    return "评估管道水锤风险，检查支吊架完整性";
                }
                return "安排超声波壁厚检测与目视检查";

            case HEAT_EXCHANGER:
                if (anomaly.getTemperatureAnomaly() > 0.6) {
                    return "评估换热管堵管率与化学污染情况，准备在线化学清洗";
                }
                return "检查一二次侧压差，评估传热管完整性";

            default:
                return "执行标准故障排查流程，联系运行工程师";
        }
    }

    private double calculateOverallConfidence(
            List<FaultTraceResult.CandidateRootCause> rootCauses,
            List<FaultTraceResult.FaultPathNode> faultPath) {

        if (rootCauses.isEmpty()) {
            return 0.1;
        }

        double topRootConfidence = rootCauses.get(0).getRootCauseProbability();
        double pathCompleteness = faultPath.isEmpty() ? 0.0 :
                Math.min(1.0, faultPath.size() / (double) maxSearchDepth);
        double anomalyCoverage = faultPath.stream()
                .filter(n -> n.getAnomalyScore() > 0.3)
                .count() / (double) Math.max(1, faultPath.size());

        return Math.min(1.0,
                topRootConfidence * 0.6
                + pathCompleteness * 0.2
                + anomalyCoverage * 0.2);
    }

    private EquipmentNode mapToEquipment(Value value) {
        if (value == null || value.isNull()) return null;
        org.neo4j.driver.types.Node node = value.asNode();
        return EquipmentNode.builder()
                .id(node.id())
                .equipmentId(node.get("equipment_id").asString(""))
                .equipmentType(EquipmentNode.EquipmentType.valueOf(
                        node.get("equipment_type").asString("OTHER")))
                .name(node.get("name").asString(""))
                .location(node.get("location").asString(""))
                .plcId(node.get("plc_id").asString(null))
                .nodeId(node.get("node_id").asString(null))
                .criticality(node.get("criticality").asDouble(0.5))
                .status(node.get("status").asString("UNKNOWN"))
                .build();
    }

    private Connection mapToConnection(Value value) {
        if (value == null || value.isNull()) return null;
        org.neo4j.driver.types.Relationship rel = value.asRelationship();
        return Connection.builder()
                .id(rel.id())
                .flowDirection(Connection.FlowDirection.valueOf(
                        rel.get("flow_direction").asString("FORWARD")))
                .propagationProbability(rel.get("propagation_probability").asDouble(0.5))
                .connectionType(rel.get("connection_type").asString(""))
                .diameterMm(rel.containsKey("diameter_mm") ?
                        rel.get("diameter_mm").asInt(0) : null)
                .lengthM(rel.containsKey("length_m") ?
                        rel.get("length_m").asInt(0) : null)
                .build();
    }

    static class BfsNode {
        final EquipmentNode equipment;
        final int distance;
        final double cumulativePropagationProb;
        final String parentId;
        final Connection connection;

        BfsNode(EquipmentNode equipment, int distance,
                double cumulativePropagationProb, String parentId, Connection connection) {
            this.equipment = equipment;
            this.distance = distance;
            this.cumulativePropagationProb = cumulativePropagationProb;
            this.parentId = parentId;
            this.connection = connection;
        }
    }
}
