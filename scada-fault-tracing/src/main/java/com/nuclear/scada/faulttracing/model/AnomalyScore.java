package com.nuclear.scada.faulttracing.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AnomalyScore {

    private String equipmentId;
    private double totalScore;
    private double pressureAnomaly;
    private double temperatureAnomaly;
    private double vibrationAnomaly;
    private double flowAnomaly;
    private Instant calculationTime;
    private Map<String, Double> componentScores;

    public static AnomalyScore normal(String equipmentId) {
        return AnomalyScore.builder()
                .equipmentId(equipmentId)
                .totalScore(0.0)
                .pressureAnomaly(0.0)
                .temperatureAnomaly(0.0)
                .vibrationAnomaly(0.0)
                .flowAnomaly(0.0)
                .calculationTime(Instant.now())
                .componentScores(new ConcurrentHashMap<>())
                .build();
    }

    public static AnomalyScore critical(String equipmentId, double score) {
        return AnomalyScore.builder()
                .equipmentId(equipmentId)
                .totalScore(score)
                .pressureAnomaly(score > 0.5 ? score * 0.8 : score)
                .temperatureAnomaly(score > 0.5 ? score * 0.7 : 0.0)
                .vibrationAnomaly(score > 0.7 ? score * 0.9 : 0.0)
                .flowAnomaly(score > 0.6 ? score * 0.85 : 0.0)
                .calculationTime(Instant.now())
                .componentScores(new ConcurrentHashMap<>())
                .build();
    }
}
