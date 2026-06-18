package com.nuclear.scada.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.nuclear.scada.common.model.SensorData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

    @Value("${scada.kafka.consumer.write-queue-capacity:50000}")
    private int writeQueueCapacity;

    @Value("${scada.kafka.consumer.write-thread-count:4}")
    private int writeThreadCount;

    @Value("${scada.kafka.consumer.max-poll-interval-ms:300000}")
    private int maxPollIntervalMs;

    @Value("${scada.kafka.consumer.session-timeout-ms:120000}")
    private int sessionTimeoutMs;

    @Value("${scada.kafka.consumer.heartbeat-interval-ms:30000}")
    private int heartbeatIntervalMs;

    @Value("${scada.kafka.consumer.max-poll-records:500}")
    private int maxPollRecords;

    @Value("${scada.kafka.consumer.retry-max-attempts:3}")
    private int retryMaxAttempts;

    @Value("${scada.kafka.consumer.retry-backoff-ms:1000}")
    private int retryBackoffMs;

    @Value("${scada.kafka.consumer.commit-interval-ms:5000}")
    private int commitIntervalMs;

    private KafkaConsumer<String, String> consumer;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong consumedCount = new AtomicLong(0);
    private final AtomicLong committedCount = new AtomicLong(0);

    private BlockingQueue<PartitionedBatch> writeQueue;
    private ExecutorService writeExecutor;
    private Thread pollThread;

    private final ConcurrentHashMap<TopicPartition, OffsetAndMetadata> pendingOffsets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TopicPartition, OffsetAndMetadata> completedOffsets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TopicPartition, Long> lastCommittedOffsets = new ConcurrentHashMap<>();

    private volatile CountDownLatch shutdownLatch;

    public SensorKafkaConsumer(SensorBatchWriter batchWriter) {
        this.batchWriter = batchWriter;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            log.warn("Kafka consumer already running");
            return;
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords));
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1024");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "100");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(maxPollIntervalMs));
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(sessionTimeoutMs));
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(heartbeatIntervalMs));
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(sessionTimeoutMs + 10000));
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "60000");
        props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "540000");
        props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, "500");
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, "500");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(TOPIC), new RebalanceListener());

        this.writeQueue = new LinkedBlockingQueue<>(writeQueueCapacity);
        this.writeExecutor = Executors.newFixedThreadPool(writeThreadCount, r -> {
            Thread t = new Thread(r, "clickhouse-io-writer");
            t.setDaemon(true);
            return t;
        });

        shutdownLatch = new CountDownLatch(1);

        for (int i = 0; i < writeThreadCount; i++) {
            writeExecutor.submit(new WriteWorker());
        }

        pollThread = new Thread(this::pollLoop, "kafka-poll-thread");
        pollThread.setDaemon(true);
        pollThread.start();

        Thread commitThread = new Thread(this::commitLoop, "kafka-commit-thread");
        commitThread.setDaemon(true);
        commitThread.start();

        log.info("Kafka consumer started with decoupled architecture: topic={}, group={}, " +
                 "maxPollInterval={}ms, sessionTimeout={}ms, writeThreads={}, queueCapacity={}",
                 TOPIC, GROUP_ID, maxPollIntervalMs, sessionTimeoutMs, writeThreadCount, writeQueueCapacity);
    }

    private void pollLoop() {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> partitionBuffer = new HashMap<>();
        long lastPollTime = System.currentTimeMillis();

        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));

                if (records.isEmpty()) {
                    flushPartitionBuffers(partitionBuffer);
                    applyBackpressureIfNeeded();
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    partitionBuffer.computeIfAbsent(tp, k -> new ArrayList<>()).add(record);
                    consumedCount.incrementAndGet();
                }

                long now = System.currentTimeMillis();
                boolean shouldFlush = false;
                for (List<ConsumerRecord<String, String>> buf : partitionBuffer.values()) {
                    if (buf.size() >= batchSize) {
                        shouldFlush = true;
                        break;
                    }
                }
                if (now - lastPollTime > 200) {
                    shouldFlush = true;
                }

                if (shouldFlush) {
                    flushPartitionBuffers(partitionBuffer);
                    lastPollTime = now;
                }

                applyBackpressureIfNeeded();
            }
        } catch (Exception e) {
            if (running.get()) {
                log.error("Kafka poll loop terminated unexpectedly", e);
            }
        } finally {
            flushPartitionBuffers(partitionBuffer);
            if (shutdownLatch != null) {
                shutdownLatch.countDown();
            }
        }
    }

    private void flushPartitionBuffers(Map<TopicPartition, List<ConsumerRecord<String, String>>> partitionBuffer) {
        for (Map.Entry<TopicPartition, List<ConsumerRecord<String, String>>> entry : partitionBuffer.entrySet()) {
            TopicPartition tp = entry.getKey();
            List<ConsumerRecord<String, String>> records = entry.getValue();
            if (records.isEmpty()) continue;

            long highestOffset = 0;
            List<SensorData> sensorDataList = new ArrayList<>(records.size());
            for (ConsumerRecord<String, String> record : records) {
                try {
                    SensorData data = objectMapper.readValue(record.value(), SensorData.class);
                    sensorDataList.add(data);
                    if (record.offset() > highestOffset) {
                        highestOffset = record.offset();
                    }
                } catch (Exception e) {
                    log.error("Failed to deserialize Kafka record: topic={}, partition={}, offset={}",
                            tp.topic(), tp.partition(), record.offset(), e);
                }
            }

            if (!sensorDataList.isEmpty()) {
                OffsetAndMetadata pendingOffset = new OffsetAndMetadata(highestOffset + 1);
                pendingOffsets.put(tp, pendingOffset);

                PartitionedBatch batch = new PartitionedBatch(tp, sensorDataList, pendingOffset);
                boolean offered = false;
                try {
                    offered = writeQueue.offer(batch, 5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }

                if (!offered) {
                    log.warn("Write queue full, pausing partition {}-{}", tp.topic(), tp.partition());
                    consumer.pause(Collections.singletonList(tp));
                    try {
                        writeQueue.put(batch);
                        consumer.resume(Collections.singletonList(tp));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        pendingOffsets.remove(tp, pendingOffset);
                        break;
                    }
                }
            }
        }
        partitionBuffer.clear();
    }

    private void applyBackpressureIfNeeded() {
        int queueSize = writeQueue.size();
        double fillRatio = (double) queueSize / writeQueueCapacity;

        if (fillRatio > 0.75) {
            log.warn("Write queue filling up: {}/{} ({}%), pausing all partitions",
                    queueSize, writeQueueCapacity, String.format("%.0f", fillRatio * 100));
            consumer.pause(consumer.assignment());
        } else if (fillRatio < 0.4) {
            consumer.resume(consumer.assignment());
        }
    }

    private class WriteWorker implements Runnable {
        @Override
        public void run() {
            while (running.get() || !writeQueue.isEmpty()) {
                try {
                    PartitionedBatch batch = writeQueue.poll(1, TimeUnit.SECONDS);
                    if (batch == null) continue;

                    boolean success = writeWithRetry(batch);

                    if (success) {
                        completedOffsets.put(batch.partition, batch.pendingOffset);
                    } else {
                        log.error("Permanently failed to write batch for partition {}-{} after {} attempts, " +
                                  "re-enqueuing for retry",
                                batch.partition.topic(), batch.partition.partition(), retryMaxAttempts);
                        try {
                            writeQueue.put(batch);
                            Thread.sleep(retryBackoffMs * 4);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception e) {
                    log.error("WriteWorker encountered unexpected error", e);
                }
            }
        }

        private boolean writeWithRetry(PartitionedBatch batch) {
            for (int attempt = 1; attempt <= retryMaxAttempts; attempt++) {
                try {
                    batchWriter.writeBatchSync(batch.sensorDataList);
                    log.debug("Successfully wrote {} records to ClickHouse for partition {}-{}",
                            batch.sensorDataList.size(),
                            batch.partition.topic(), batch.partition.partition());
                    return true;
                } catch (Exception e) {
                    log.error("ClickHouse write attempt {}/{} failed for partition {}-{}, batch size={}",
                            attempt, retryMaxAttempts,
                            batch.partition.topic(), batch.partition.partition(),
                            batch.sensorDataList.size(), e);
                    if (attempt < retryMaxAttempts) {
                        try {
                            Thread.sleep(retryBackoffMs * attempt);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return false;
                        }
                    }
                }
            }
            return false;
        }
    }

    private void commitLoop() {
        while (running.get()) {
            try {
                Thread.sleep(commitIntervalMs);
                commitCompletedOffsets();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private synchronized void commitCompletedOffsets() {
        if (completedOffsets.isEmpty()) return;

        Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : completedOffsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            OffsetAndMetadata offset = entry.getValue();
            Long lastCommitted = lastCommittedOffsets.get(tp);

            if (lastCommitted == null || offset.offset() > lastCommitted) {
                OffsetAndMetadata pending = pendingOffsets.get(tp);
                if (pending != null && pending.offset() > offset.offset()) {
                    toCommit.put(tp, offset);
                } else if (pending == null) {
                    toCommit.put(tp, offset);
                } else {
                    toCommit.put(tp, pending);
                }
            }
        }

        if (toCommit.isEmpty()) return;

        try {
            consumer.commitSync(toCommit);
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : toCommit.entrySet()) {
                TopicPartition tp = entry.getKey();
                long committedOffset = entry.getValue().offset();
                lastCommittedOffsets.put(tp, committedOffset);
                completedOffsets.remove(tp);
                pendingOffsets.remove(tp);
                committedCount.addAndGet(1);
            }
            log.debug("Committed offsets for {} partitions: {}", toCommit.size(), toCommit);
        } catch (Exception e) {
            log.error("Failed to commit offsets, will retry next cycle", e);
        }
    }

    private class RebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            if (partitions.isEmpty()) return;
            log.warn("Partitions REVOKED: {}. Draining write queue and committing final offsets...", partitions);

            drainWriteQueue();

            synchronized (SensorKafkaConsumer.this) {
                Map<TopicPartition, OffsetAndMetadata> finalCommit = new HashMap<>();
                for (TopicPartition tp : partitions) {
                    OffsetAndMetadata completed = completedOffsets.get(tp);
                    OffsetAndMetadata pending = pendingOffsets.get(tp);
                    OffsetAndMetadata toCommit = null;
                    if (completed != null && pending != null) {
                        toCommit = completed.offset() >= pending.offset() ? completed : pending;
                    } else if (completed != null) {
                        toCommit = completed;
                    } else if (pending != null) {
                        toCommit = pending;
                    }
                    if (toCommit != null) {
                        finalCommit.put(tp, toCommit);
                    }
                }
                if (!finalCommit.isEmpty()) {
                    try {
                        consumer.commitSync(finalCommit);
                        log.info("Committed final offsets during rebalance revoke: {}", finalCommit);
                    } catch (Exception e) {
                        log.error("Failed to commit offsets during partition revocation", e);
                    }
                }
            }

            for (TopicPartition tp : partitions) {
                pendingOffsets.remove(tp);
                completedOffsets.remove(tp);
                lastCommittedOffsets.remove(tp);
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            if (partitions.isEmpty()) return;
            log.info("Partitions ASSIGNED: {}. Resetting offset tracking.", partitions);
            for (TopicPartition tp : partitions) {
                pendingOffsets.remove(tp);
                completedOffsets.remove(tp);
                lastCommittedOffsets.remove(tp);
            }
        }
    }

    private void drainWriteQueue() {
        int drained = 0;
        List<PartitionedBatch> remaining = new ArrayList<>();
        writeQueue.drainTo(remaining);
        for (PartitionedBatch batch : remaining) {
            try {
                batchWriter.writeBatchSync(batch.sensorDataList);
                completedOffsets.put(batch.partition, batch.pendingOffset);
                drained++;
            } catch (Exception e) {
                log.error("Failed to drain batch for partition {}-{}, data may be lost!",
                        batch.partition.topic(), batch.partition.partition(), e);
                try {
                    writeQueue.put(batch);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        if (drained > 0) {
            log.info("Drained {} batches from write queue during rebalance", drained);
        }
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        log.info("Shutting down Kafka consumer gracefully...");

        try {
            if (pollThread != null) {
                pollThread.interrupt();
                pollThread.join(10000);
            }

            if (shutdownLatch != null) {
                shutdownLatch.await(30, TimeUnit.SECONDS);
            }

            drainWriteQueue();
            commitCompletedOffsets();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (writeExecutor != null) {
                writeExecutor.shutdown();
                try {
                    if (!writeExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                        writeExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    writeExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }

            if (consumer != null) {
                try {
                    consumer.close(Duration.ofSeconds(10));
                } catch (Exception e) {
                    log.error("Error closing Kafka consumer", e);
                }
            }

            log.info("Kafka consumer stopped. consumed={}, committed={}", consumedCount.get(), committedCount.get());
        }
    }

    public long getConsumedCount() {
        return consumedCount.get();
    }

    public long getCommittedCount() {
        return committedCount.get();
    }

    public int getWriteQueueSize() {
        return writeQueue != null ? writeQueue.size() : 0;
    }

    public Map<String, Object> getConsumerMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("consumedCount", consumedCount.get());
        metrics.put("committedCount", committedCount.get());
        metrics.put("writeQueueSize", getWriteQueueSize());
        metrics.put("pendingOffsets", pendingOffsets.size());
        metrics.put("completedOffsets", completedOffsets.size());
        return metrics;
    }

    static class PartitionedBatch {
        final TopicPartition partition;
        final List<SensorData> sensorDataList;
        final OffsetAndMetadata pendingOffset;

        PartitionedBatch(TopicPartition partition, List<SensorData> sensorDataList,
                         OffsetAndMetadata pendingOffset) {
            this.partition = partition;
            this.sensorDataList = sensorDataList;
            this.pendingOffset = pendingOffset;
        }
    }
}
