package com.nuclear.scada.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.nuclear.scada.common.model.SensorData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.Future;

@Slf4j
@Component
public class SensorKafkaProducer {

    private static final String TOPIC = "scada-sensor-data";

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;

    public SensorKafkaProducer(@Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        this.producer = new KafkaProducer<>(props);
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    public void publish(SensorData data) {
        try {
            String json = objectMapper.writeValueAsString(data);
            String key = data.getPlcId() + ":" + data.getNodeId();
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, json);
            Future<RecordMetadata> future = producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to publish sensor data to Kafka: plcId={}, nodeId={}",
                            data.getPlcId(), data.getNodeId(), exception);
                }
            });
        } catch (Exception e) {
            log.error("Error serializing sensor data: plcId={}, nodeId={}",
                    data.getPlcId(), data.getNodeId(), e);
        }
    }

    public void flush() {
        producer.flush();
    }

    public void close() {
        producer.close();
    }
}
