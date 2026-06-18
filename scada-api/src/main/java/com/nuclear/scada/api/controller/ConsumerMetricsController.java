package com.nuclear.scada.api.controller;

import com.nuclear.scada.kafka.SensorKafkaConsumer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/consumer")
@RequiredArgsConstructor
public class ConsumerMetricsController {

    private final SensorKafkaConsumer kafkaConsumer;

    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> getConsumerMetrics() {
        return ResponseEntity.ok(kafkaConsumer.getConsumerMetrics());
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getConsumerHealth() {
        Map<String, Object> metrics = kafkaConsumer.getConsumerMetrics();
        int queueSize = (int) metrics.getOrDefault("writeQueueSize", 0);
        int pendingOffsets = (int) metrics.getOrDefault("pendingOffsets", 0);

        String status;
        if (queueSize < 10000 && pendingOffsets < 10) {
            status = "HEALTHY";
        } else if (queueSize < 30000 && pendingOffsets < 50) {
            status = "WARNING";
        } else {
            status = "CRITICAL";
        }

        metrics.put("status", status);
        return ResponseEntity.ok(metrics);
    }
}
