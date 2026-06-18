package com.nuclear.scada.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.nuclear.scada.clickhouse.SensorBatchWriter;
import com.nuclear.scada.common.model.SensorData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class SensorKafkaConsumer {

    private static final String TOPIC = "scada-sensor-data";
    private static final String GROUP_ID = "scada-clickhouse-writer";

    private final SensorBatchWriter batchWriter;
    private final ObjectMapper objectMapper;

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${scada.kafka.consumer.poll-timeout-ms:100}")
    private int pollTimeoutMs;

    @Value("${scada.kafka.consumer.batch-size:5000}")
    private int batchSize;

    @Value("${scada.kafka.consumer.flush-interval-ms:500}")
    private int flushIntervalMs;

    private final CopyOnWriteArrayList<SensorData> buffer = new CopyOnWriteArrayList<>();
    private final AtomicLong consumedCount = new AtomicLong(0);
    private KafkaConsumer<String, String> consumer;
    private ScheduledExecutorService scheduler;

    public SensorKafkaConsumer(SensorBatchWriter batchWriter) {
        this.batchWriter = batchWriter;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    public void start() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1024");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "100");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(TOPIC));

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kafka-consumer-flush");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::flushBuffer, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);

        Thread consumeThread = new Thread(this::consumeLoop, "kafka-consumer-loop");
        consumeThread.setDaemon(true);
        consumeThread.start();

        log.info("Kafka consumer started, topic={}, group={}", TOPIC, GROUP_ID);
    }

    private void consumeLoop() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        SensorData data = objectMapper.readValue(record.value(), SensorData.class);
                        buffer.add(data);
                        consumedCount.incrementAndGet();

                        if (buffer.size() >= batchSize) {
                            flushBuffer();
                        }
                    } catch (Exception e) {
                        log.error("Failed to deserialize sensor data from Kafka: offset={}", record.offset(), e);
                    }
                }
                consumer.commitSync();
            }
        } catch (Exception e) {
            log.error("Kafka consume loop terminated", e);
        }
    }

    private void flushBuffer() {
        if (buffer.isEmpty()) return;

        List<SensorData> batch = new ArrayList<>();
        int count = 0;
        for (SensorData data : buffer) {
            if (count >= batchSize) break;
            batch.add(data);
            count++;
        }
        buffer.removeAll(batch);

        if (!batch.isEmpty()) {
            try {
                batchWriter.writeBatch(batch);
                log.debug("Flushed {} sensor records to ClickHouse", batch.size());
            } catch (Exception e) {
                log.error("Failed to write batch to ClickHouse, size={}", batch.size(), e);
                buffer.addAll(batch);
            }
        }
    }

    public long getConsumedCount() {
        return consumedCount.get();
    }

    public void stop() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
        flushBuffer();
        if (consumer != null) {
            consumer.close();
        }
    }
}
