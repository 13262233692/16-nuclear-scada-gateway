package com.nuclear.scada.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SensorData {

    private String plcId;
    private String nodeId;
    private SensorType sensorType;
    private double value;
    private String unit;
    private Instant timestamp;
    private QualityCode qualityCode;

    public enum SensorType {
        COOLANT_PRESSURE,
        MAIN_PUMP_SPEED,
        EXTREME_TEMPERATURE
    }

    public enum QualityCode {
        GOOD(0),
        UNCERTAIN(1),
        BAD(2);

        private final int code;

        QualityCode(int code) {
            this.code = code;
        }

        public static QualityCode fromCode(int code) {
            for (QualityCode qc : values()) {
                if (qc.code == code) return qc;
            }
            return BAD;
        }
    }
}
