package com.nuclear.scada.codec;

import com.nuclear.scada.common.model.CoolantPressureReading;
import com.nuclear.scada.common.model.ExtremeTemperatureReading;
import com.nuclear.scada.common.model.MainPumpSpeedReading;
import com.nuclear.scada.common.model.SensorData;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class SensorDataExtractor {

    public ExtractionResult extract(List<SensorData> sensorDataList) {
        List<CoolantPressureReading> pressures = new ArrayList<>();
        List<MainPumpSpeedReading> pumpSpeeds = new ArrayList<>();
        List<ExtremeTemperatureReading> temperatures = new ArrayList<>();

        for (SensorData sd : sensorDataList) {
            switch (sd.getSensorType()) {
                case COOLANT_PRESSURE:
                    pressures.add(new CoolantPressureReading(
                            sd.getPlcId(),
                            sd.getNodeId(),
                            sd.getValue(),
                            sd.getTimestamp(),
                            sd.getQualityCode()
                    ));
                    break;
                case MAIN_PUMP_SPEED:
                    pumpSpeeds.add(new MainPumpSpeedReading(
                            sd.getPlcId(),
                            sd.getNodeId(),
                            sd.getValue(),
                            sd.getTimestamp(),
                            sd.getQualityCode()
                    ));
                    break;
                case EXTREME_TEMPERATURE:
                    temperatures.add(new ExtremeTemperatureReading(
                            sd.getPlcId(),
                            sd.getNodeId(),
                            sd.getValue(),
                            extractProbeLocation(sd.getNodeId()),
                            sd.getTimestamp(),
                            sd.getQualityCode()
                    ));
                    break;
            }
        }

        return new ExtractionResult(pressures, pumpSpeeds, temperatures);
    }

    private String extractProbeLocation(String nodeId) {
        if (nodeId == null) return "UNKNOWN";
        String[] parts = nodeId.split("\\.");
        return parts.length > 1 ? parts[parts.length - 1] : nodeId;
    }

    public static class ExtractionResult {
        private final List<CoolantPressureReading> pressures;
        private final List<MainPumpSpeedReading> pumpSpeeds;
        private final List<ExtremeTemperatureReading> temperatures;

        public ExtractionResult(List<CoolantPressureReading> pressures,
                                List<MainPumpSpeedReading> pumpSpeeds,
                                List<ExtremeTemperatureReading> temperatures) {
            this.pressures = pressures;
            this.pumpSpeeds = pumpSpeeds;
            this.temperatures = temperatures;
        }

        public List<CoolantPressureReading> getPressures() { return pressures; }
        public List<MainPumpSpeedReading> getPumpSpeeds() { return pumpSpeeds; }
        public List<ExtremeTemperatureReading> getTemperatures() { return temperatures; }
    }
}
