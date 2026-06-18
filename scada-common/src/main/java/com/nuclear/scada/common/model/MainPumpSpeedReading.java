package com.nuclear.scada.common.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MainPumpSpeedReading {
    private String plcId;
    private String nodeId;
    private double rpm;
    private Instant timestamp;
    private SensorData.QualityCode quality;
}
