package com.nuclear.scada.kafka;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumerRunner {

    private final SensorKafkaConsumer kafkaConsumer;

    @PostConstruct
    public void init() {
        kafkaConsumer.start();
    }

    @PreDestroy
    public void destroy() {
        kafkaConsumer.stop();
    }
}
