package com.nuclear.scada.common.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExtremeTemperatureReading {
    private String plcId;
    private String nodeId;
    private double temperatureCelsius;
    private String probeLocation;
    private Instant timestamp;
    private SensorData.QualityCode quality;
}
