package com.nuclear.scada.kafka;

import com.nuclear.scada.clickhouse.SensorBatchWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumerRunner {

    private final SensorKafkaConsumer kafkaConsumer;
    private final SensorBatchWriter batchWriter;

    @PostConstruct
    public void init() {
        kafkaConsumer.start();
    }

    @PreDestroy
    public void destroy() {
        kafkaConsumer.stop();
        batchWriter.shutdown();
    }
}
