package com.nuclear.scada.faulttracing.service;

import com.nuclear.scada.common.model.SensorData;
import com.nuclear.scada.faulttracing.model.AnomalyScore;
import com.nuclear.scada.faulttracing.model.EquipmentNode;
import com.nuclear.scada.clickhouse.SensorQueryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
@Slf4j
public class AnomalyDetectionService {

    private final SensorQueryRepository sensorRepository;

    private static final double MIN_PRESSURE_MPA = 15.0;
    private static final double MAX_PRESSURE_MPA = 16.5;
    private static final double MIN_PUMP_RPM = 1480;
    private static final double MAX_PUMP_RPM = 1520;
    private static final double MAX_TEMPERATURE_C = 320;
    private static final double TEMPERATURE_THRESHOLD_C = 300;

    public AnomalyScore calculateAnomalyScore(EquipmentNode equipment) {
        return calculateAnomalyScore(equipment, Duration.ofMinutes(5));
    }

    public AnomalyScore calculateAnomalyScore(EquipmentNode equipment, Duration timeWindow) {
        Instant end = Instant.now();
        Instant start = end.minus(timeWindow);

        String plcId = equipment.getPlcId();
        if (plcId == null || plcId.isEmpty()) {
            log.warn("No PLC ID associated with equipment: {}", equipment.getEquipmentId());
            return AnomalyScore.normal(equipment.getEquipmentId());
        }

        EquipmentNode.EquipmentType type = equipment.getEquipmentType();
        AnomalyScore score = AnomalyScore.normal(equipment.getEquipmentId());
        Map<String, Double> components = new ConcurrentHashMap<>();

        switch (type) {
            case PIPE:
            case HEAT_EXCHANGER:
            case SENSOR:
                analyzePressureReadings(plcId, start, end, score, components);
                analyzeTemperatureReadings(plcId, start, end, score, components);
                break;
            case VALVE:
                analyzePressureReadings(plcId, start, end, score, components);
                analyzeFlowBehavior(plcId, start, end, score, components);
                break;
            case PUMP:
                analyzePumpSpeedReadings(plcId, start, end, score, components);
                analyzePressureReadings(plcId, start, end, score, components);
                analyzeVibration(plcId, start, end, score, components);
                break;
            default:
                analyzePressureReadings(plcId, start, end, score, components);
                analyzeTemperatureReadings(plcId, start, end, score, components);
                break;
        }

        score.setComponentScores(components);
        score.setTotalScore(calculateWeightedTotal(score, type));

        log.debug("Anomaly score for {} [{}]: {}", equipment.getEquipmentId(), type, score.getTotalScore());
        return score;
    }

    private void analyzePressureReadings(String plcId, Instant start, Instant end,
                                         AnomalyScore score, Map<String, Double> components) {
        try {
            List<SensorData> pressureData = sensorRepository.queryByTimeRange(
                    plcId, "COOLANT_PRESSURE", start, end);

            if (pressureData.isEmpty()) {
                components.put("pressure_data_availability", 0.1);
                return;
            }

            double sum = 0;
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            int anomalyCount = 0;
            double prev = Double.NaN;
            double maxDrop = 0;

            for (SensorData d : pressureData) {
                double val = d.getValue();
                sum += val;
                if (val < min) min = val;
                if (val > max) max = val;
                if (val < MIN_PRESSURE_MPA || val > MAX_PRESSURE_MPA) {
                    anomalyCount++;
                }
                if (!Double.isNaN(prev)) {
                    double drop = prev - val;
                    if (drop > maxDrop) maxDrop = drop;
                }
                prev = val;
            }

            double avg = sum / pressureData.size();
            double range = max - min;
            double anomalyRatio = (double) anomalyCount / pressureData.size();
            double suddenDropScore = maxDrop > 0.5 ? Math.min(1.0, maxDrop / 2.0) : 0;

            double pressureScore = 0;
            if (avg < MIN_PRESSURE_MPA) {
                pressureScore = Math.min(1.0, (MIN_PRESSURE_MPA - avg) / 3.0 + 0.5);
            } else if (avg > MAX_PRESSURE_MPA) {
                pressureScore = Math.min(1.0, (avg - MAX_PRESSURE_MPA) / 3.0 + 0.5);
            }

            pressureScore = Math.max(pressureScore, anomalyRatio * 0.7);
            pressureScore = Math.max(pressureScore, suddenDropScore);
            pressureScore = Math.max(pressureScore, range > 2 ? Math.min(1.0, range / 5.0) : 0);

            score.setPressureAnomaly(pressureScore);
            components.put("pressure_avg", avg);
            components.put("pressure_min", min);
            components.put("pressure_max", max);
            components.put("pressure_anomaly_score", pressureScore);
            components.put("pressure_sudden_drop", maxDrop);

        } catch (Exception e) {
            log.warn("Error analyzing pressure for PLC {}: {}", plcId, e.getMessage());
            score.setPressureAnomaly(0.1);
        }
    }

    private void analyzeTemperatureReadings(String plcId, Instant start, Instant end,
                                            AnomalyScore score, Map<String, Double> components) {
        try {
            List<SensorData> tempData = sensorRepository.queryByTimeRange(
                    plcId, "EXTREME_TEMPERATURE", start, end);

            if (tempData.isEmpty()) {
                components.put("temp_data_availability", 0.1);
                return;
            }

            double sum = 0;
            double max = Double.MIN_VALUE;
            int overThreshold = 0;

            for (SensorData d : tempData) {
                double val = d.getValue();
                sum += val;
                if (val > max) max = val;
                if (val > TEMPERATURE_THRESHOLD_C) overThreshold++;
            }

            double avg = sum / tempData.size();
            double tempScore = 0;

            if (max > MAX_TEMPERATURE_C) {
                tempScore = Math.min(1.0, (max - MAX_TEMPERATURE_C) / 50.0 + 0.6);
            } else if (avg > TEMPERATURE_THRESHOLD_C) {
                tempScore = Math.min(1.0, (avg - TEMPERATURE_THRESHOLD_C) / 30.0 + 0.3);
            }

            double overRatio = (double) overThreshold / tempData.size();
            tempScore = Math.max(tempScore, overRatio * 0.7);

            score.setTemperatureAnomaly(tempScore);
            components.put("temperature_avg", avg);
            components.put("temperature_max", max);
            components.put("temperature_anomaly_score", tempScore);

        } catch (Exception e) {
            log.warn("Error analyzing temperature for PLC {}: {}", plcId, e.getMessage());
            score.setTemperatureAnomaly(0.1);
        }
    }

    private void analyzePumpSpeedReadings(String plcId, Instant start, Instant end,
                                          AnomalyScore score, Map<String, Double> components) {
        try {
            List<SensorData> speedData = sensorRepository.queryByTimeRange(
                    plcId, "MAIN_PUMP_SPEED", start, end);

            if (speedData.isEmpty()) {
                components.put("pump_speed_availability", 0.1);
                score.setFlowAnomaly(0.1);
                return;
            }

            double sum = 0;
            double prev = Double.NaN;
            double maxVariation = 0;
            int belowNormal = 0;
            int zeroSpeed = 0;

            for (SensorData d : speedData) {
                double val = d.getValue();
                sum += val;
                if (val < MIN_PUMP_RPM) belowNormal++;
                if (val < 10) zeroSpeed++;
                if (!Double.isNaN(prev)) {
                    double variation = Math.abs(val - prev);
                    if (variation > maxVariation) maxVariation = variation;
                }
                prev = val;
            }

            double avg = sum / speedData.size();
            double speedScore = 0;

            if (zeroSpeed > 0) {
                speedScore = 1.0;
            } else if (avg < MIN_PUMP_RPM) {
                speedScore = Math.min(1.0, (MIN_PUMP_RPM - avg) / 200.0 + 0.3);
            } else if (avg > MAX_PUMP_RPM) {
                speedScore = Math.min(1.0, (avg - MAX_PUMP_RPM) / 200.0 + 0.2);
            }

            double variationScore = maxVariation > 50 ? Math.min(1.0, maxVariation / 200.0) : 0;
            double belowRatio = (double) belowNormal / speedData.size();
            speedScore = Math.max(speedScore, belowRatio * 0.8);
            speedScore = Math.max(speedScore, variationScore * 0.6);

            score.setFlowAnomaly(speedScore);
            components.put("pump_speed_avg", avg);
            components.put("pump_speed_variation", maxVariation);
            components.put("pump_speed_anomaly_score", speedScore);

        } catch (Exception e) {
            log.warn("Error analyzing pump speed for PLC {}: {}", plcId, e.getMessage());
            score.setFlowAnomaly(0.1);
        }
    }

    private void analyzeVibration(String plcId, Instant start, Instant end,
                                  AnomalyScore score, Map<String, Double> components) {
        try {
            List<SensorData> speedData = sensorRepository.queryByTimeRange(
                    plcId, "MAIN_PUMP_SPEED", start, end);

            if (speedData.size() < 2) {
                score.setVibrationAnomaly(0.1);
                return;
            }

            double sum = 0;
            double prev = Double.NaN;
            double totalVariation = 0;

            for (SensorData d : speedData) {
                double val = d.getValue();
                sum += val;
                if (!Double.isNaN(prev)) {
                    totalVariation += Math.abs(val - prev);
                }
                prev = val;
            }

            double avgVariation = totalVariation / (speedData.size() - 1);
            double vibrationScore = avgVariation > 10 ? Math.min(1.0, avgVariation / 50.0) : 0;

            score.setVibrationAnomaly(vibrationScore);
            components.put("vibration_score", vibrationScore);
            components.put("avg_speed_variation", avgVariation);

        } catch (Exception e) {
            score.setVibrationAnomaly(0.0);
        }
    }

    private void analyzeFlowBehavior(String plcId, Instant start, Instant end,
                                     AnomalyScore score, Map<String, Double> components) {
        try {
            List<SensorData> pressureData = sensorRepository.queryByTimeRange(
                    plcId, "COOLANT_PRESSURE", start, end);

            if (pressureData.size() < 3) {
                score.setFlowAnomaly(0.1);
                return;
            }

            double prev = Double.NaN;
            double trend = 0;
            double oscillationCount = 0;
            boolean wasDecreasing = false;
            boolean wasIncreasing = false;

            for (int i = 1; i < pressureData.size(); i++) {
                double current = pressureData.get(i).getValue();
                double last = pressureData.get(i - 1).getValue();
                double diff = current - last;

                if (diff < -0.05) {
                    if (wasIncreasing) oscillationCount++;
                    wasDecreasing = true;
                    wasIncreasing = false;
                } else if (diff > 0.05) {
                    if (wasDecreasing) oscillationCount++;
                    wasIncreasing = true;
                    wasDecreasing = false;
                }

                trend += diff;
            }

            double oscillationScore = oscillationCount > 5 ? Math.min(1.0, oscillationCount / 15.0) : 0;
            double trendScore = trend < -1.0 ? Math.min(1.0, Math.abs(trend) / 3.0) : 0;

            double flowScore = Math.max(oscillationScore, trendScore);
            score.setFlowAnomaly(flowScore);
            components.put("flow_oscillation_score", oscillationScore);
            components.put("flow_trend_score", trendScore);
            components.put("flow_total_trend", trend);

        } catch (Exception e) {
            score.setFlowAnomaly(0.0);
        }
    }

    private double calculateWeightedTotal(AnomalyScore score, EquipmentNode.EquipmentType type) {
        double total;
        switch (type) {
            case VALVE:
                total = score.getPressureAnomaly() * 0.45
                        + score.getFlowAnomaly() * 0.45
                        + score.getTemperatureAnomaly() * 0.10;
                break;
            case PUMP:
                total = score.getFlowAnomaly() * 0.40
                        + score.getVibrationAnomaly() * 0.35
                        + score.getPressureAnomaly() * 0.20
                        + score.getTemperatureAnomaly() * 0.05;
                break;
            case PIPE:
            case SENSOR:
                total = score.getPressureAnomaly() * 0.50
                        + score.getTemperatureAnomaly() * 0.50;
                break;
            case HEAT_EXCHANGER:
                total = score.getTemperatureAnomaly() * 0.50
                        + score.getPressureAnomaly() * 0.35
                        + score.getFlowAnomaly() * 0.15;
                break;
            default:
                total = (score.getPressureAnomaly() + score.getTemperatureAnomaly()
                         + score.getFlowAnomaly() + score.getVibrationAnomaly()) / 4.0;
                break;
        }
        return Math.min(1.0, Math.max(0.0, total));
    }
}
