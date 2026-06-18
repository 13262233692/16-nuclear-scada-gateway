package com.nuclear.scada.faulttracing.config;

import com.nuclear.scada.faulttracing.model.Connection;
import com.nuclear.scada.faulttracing.model.EquipmentNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class TopologyInitializer {

    private final Driver neo4jDriver;

    private static final String CREATE_EQUIPMENT_ID_INDEX =
            "CREATE INDEX equipment_id_idx IF NOT EXISTS FOR (e:Equipment) ON (e.equipment_id)";

    private static final String CREATE_EQUIPMENT_TYPE_INDEX =
            "CREATE INDEX equipment_type_idx IF NOT EXISTS FOR (e:Equipment) ON (e.equipment_type)";

    private static final String CREATE_PLC_ID_INDEX =
            "CREATE INDEX plc_id_idx IF NOT EXISTS FOR (e:Equipment) ON (e.plc_id)";

    private static final String CREATE_CONNECTION_TYPE_INDEX =
            "CREATE INDEX conn_type_idx IF NOT EXISTS FOR ()-[r:CONNECTS_TO]-() ON (r.connection_type)";

    @EventListener(ApplicationReadyEvent.class)
    public void initTopology() {
        log.info("Initializing Neo4j physical topology schema...");

        try (Session session = neo4jDriver.session()) {
            session.executeWrite(tx -> {
                tx.run(CREATE_EQUIPMENT_ID_INDEX);
                tx.run(CREATE_EQUIPMENT_TYPE_INDEX);
                tx.run(CREATE_PLC_ID_INDEX);
                tx.run(CREATE_CONNECTION_TYPE_INDEX);
                return null;
            });

            Long count = session.executeRead(tx ->
                tx.run("MATCH (e:Equipment) RETURN count(e) AS cnt")
                        .single().get("cnt").asLong()
            );

            if (count == 0) {
                log.info("No existing topology found. Inserting sample secondary loop topology...");
                insertSampleTopology(session);
            } else {
                log.info("Existing topology found with {} equipment nodes", count);
            }
        } catch (Exception e) {
            log.warn("Neo4j topology initialization failed (may already exist): {}", e.getMessage());
        }
    }

    private void insertSampleTopology(Session session) {
        session.executeWrite(tx -> {
            tx.run("CREATE (p1:Equipment:PUMP {equipment_id: 'PUMP-MAIN-01', name: '主冷却泵01', equipment_type: 'PUMP', location: '反应堆厂房-1层', plc_id: 'PLC-001', node_id: 'ns=2;s=PUMP01', criticality: 0.95, status: 'NORMAL'})");
            tx.run("CREATE (p2:Equipment:PUMP {equipment_id: 'PUMP-MAIN-02', name: '主冷却泵02', equipment_type: 'PUMP', location: '反应堆厂房-1层', plc_id: 'PLC-001', node_id: 'ns=2;s=PUMP02', criticality: 0.95, status: 'NORMAL'})");
            tx.run("CREATE (v1:Equipment:VALVE {equipment_id: 'VALVE-INLET-01', name: '主给水入口阀01', equipment_type: 'VALVE', location: '反应堆厂房-2层', plc_id: 'PLC-002', node_id: 'ns=2;s=VLV01', criticality: 0.85, status: 'NORMAL'})");
            tx.run("CREATE (v2:Equipment:VALVE {equipment_id: 'VALVE-OUTLET-01', name: '主回路出口阀01', equipment_type: 'VALVE', location: '反应堆厂房-2层', plc_id: 'PLC-002', node_id: 'ns=2;s=VLV02', criticality: 0.85, status: 'NORMAL'})");
            tx.run("CREATE (v3:Equipment:VALVE {equipment_id: 'VALVE-REG-01', name: '压力调节阀01', equipment_type: 'VALVE', location: '蒸汽发生器间', plc_id: 'PLC-003', node_id: 'ns=2;s=VLV03', criticality: 0.90, status: 'NORMAL'})");
            tx.run("CREATE (v4:Equipment:VALVE {equipment_id: 'VALVE-ISOL-01', name: '安全隔离阀01', equipment_type: 'VALVE', location: '安全壳内', plc_id: 'PLC-003', node_id: 'ns=2;s=VLV04', criticality: 0.98, status: 'NORMAL'})");
            tx.run("CREATE (pipe1:Equipment:PIPE {equipment_id: 'PIPE-HOT-01', name: '热管段01', equipment_type: 'PIPE', location: '反应堆厂房', plc_id: 'PLC-004', node_id: 'ns=2;s=PIPE01', criticality: 0.92, status: 'NORMAL'})");
            tx.run("CREATE (pipe2:Equipment:PIPE {equipment_id: 'PIPE-COLD-01', name: '冷管段01', equipment_type: 'PIPE', location: '反应堆厂房', plc_id: 'PLC-004', node_id: 'ns=2;s=PIPE02', criticality: 0.92, status: 'NORMAL'})");
            tx.run("CREATE (pipe3:Equipment:PIPE {equipment_id: 'PIPE-FEED-01', name: '给水管01', equipment_type: 'PIPE', location: '蒸汽发生器间', plc_id: 'PLC-004', node_id: 'ns=2;s=PIPE03', criticality: 0.80, status: 'NORMAL'})");
            tx.run("CREATE (hx1:Equipment:HEAT_EXCHANGER {equipment_id: 'HX-SG-01', name: '蒸汽发生器01', equipment_type: 'HEAT_EXCHANGER', location: '蒸汽发生器间', plc_id: 'PLC-005', node_id: 'ns=2;s=HX01', criticality: 0.90, status: 'NORMAL'})");
            tx.run("CREATE (s1:Equipment:SENSOR {equipment_id: 'SENSOR-PRESS-01', name: '压力传感器01', equipment_type: 'SENSOR', location: '热管段01', plc_id: 'PLC-001', node_id: 'ns=2;s=PRESS01', criticality: 0.85, status: 'NORMAL'})");
            tx.run("CREATE (s2:Equipment:SENSOR {equipment_id: 'SENSOR-PRESS-02', name: '压力传感器02', equipment_type: 'SENSOR', location: '冷管段01', plc_id: 'PLC-001', node_id: 'ns=2;s=PRESS02', criticality: 0.85, status: 'NORMAL'})");
            tx.run("CREATE (s3:Equipment:SENSOR {equipment_id: 'SENSOR-TEMP-01', name: '温度探针01', equipment_type: 'SENSOR', location: '热管段01出口', plc_id: 'PLC-002', node_id: 'ns=2;s=TEMP01', criticality: 0.80, status: 'NORMAL'})");
            tx.run("CREATE (s4:Equipment:SENSOR {equipment_id: 'SENSOR-TEMP-02', name: '温度探针02', equipment_type: 'SENSOR', location: '蒸汽发生器01入口', plc_id: 'PLC-002', node_id: 'ns=2;s=TEMP02', criticality: 0.80, status: 'NORMAL'})");

            tx.run("MATCH (a:Equipment {equipment_id: 'VALVE-INLET-01'}), (b:Equipment {equipment_id: 'PIPE-FEED-01'}) CREATE (a)-[r:CONNECTS_TO {flow_direction: 'FORWARD', propagation_probability: 0.70, connection_type: 'FLANGE', diameter_mm: 600, length_m: 15}]->(b)");
            tx.run("MATCH (a:Equipment {equipment_id: 'PIPE-FEED-01'}), (b:Equipment {equipment_id: 'HX-SG-01'}) CREATE (a)-[r:CONNECTS_TO {flow_direction: 'FORWARD', propagation_probability: 0.85, connection_type: 'WELD', diameter_mm: 600, length_m: 30}]->(b)");
            tx.run("MATCH (a:Equipment {equipment_id: 'HX-SG-01'}), (b:Equipment {equipment_id: 'VALVE-REG-01'}) CREATE (a)-[r:CONNECTS_TO {flow_direction: 'FORWARD', propagation_probability: 0.90, connection_type: 'FLANGE', diameter_mm: 700, length_m: 10}]->(b)");
            tx.run("MATCH (a:Equipment {equipment_id: 'VALVE-REG-01'}), (b:Equipment {equipment_id: 'PIPE-HOT-01'}) CREATE (a)-[r:CONNECTS_TO {flow_direction: 'FORWARD', propagation_probability: 0.95, connection_type: 'WELD', diameter_mm: 700, length_m: 20}]->(b)");
            tx.run("MATCH (a:Equipment {equipment_id: 'PIPE-HOT-01'}), (b:Equipment {equipment_id: 'SENSOR-PRESS-01'}) CREATE (a)-[r:CONNECTS_TO {flow_direction: 'BIDIRECTIONAL', propagation_probability: 0.99, connection_type: 'INSTRUMENT', diameter_mm: 50, length_m: 2}]->(b)");
            tx.run("MATCH (a:Equipment {equipment_id: 'PIPE-HOT-01'}), (b:Equipment {equipment_id: 'SENSOR-TEMP-01'}) CREATE (a)-[r:CONNECTS_TO {flow_direction: 'BIDIRECTIONAL', propagation_probability: 0.99, connection_type: 'INSTRUMENT', diameter_mm: 50, length_m: 2}]->(b)");
            tx.run("MATCH (a:Equipment {equipment_id: 'PIPE-HOT-01'}), (b:Equipment {equipment_id: 'PUMP-MAIN-01'}) CREATE (a)-[r:CONNECTS_TO {flow_direction: 'FORWARD', propagation_probability: 0.98, connection_type: 'FLANGE', diameter_mm: 700, length_m: 25}]->(b)");
            tx.run("MATCH (a:Equipment {equipment_id: 'PUMP-MAIN-01'}), (b:Equipment {equipment_id: 'PIPE-COLD-01'}) CREATE (a)-[r:CONNECTS_TO {flow_direction: 'FORWARD', propagation_probability: 0.95, connection_type: 'WELD', diameter_mm: 700, length_m: 25}]->(b)");
            tx.run("MATCH (a:Equipment {equipment_id: 'PIPE-COLD-01'}), (b:Equipment {equipment_id: 'SENSOR-PRESS-02'}) CREATE (a)-[r:CONNECTS_TO {flow_direction: 'BIDIRECTIONAL', propagation_probability: 0.99, connection_type: 'INSTRUMENT', diameter_mm: 50, length_m: 2}]->(b)");
            tx.run("MATCH (a:Equipment {equipment_id: 'PIPE-COLD-01'}), (b:Equipment {equipment_id: 'VALVE-ISOL-01'}) CREATE (a)-[r:CONNECTS_TO {flow_direction: 'FORWARD', propagation_probability: 0.90, connection_type: 'FLANGE', diameter_mm: 700, length_m: 15}]->(b)");
            tx.run("MATCH (a:Equipment {equipment_id: 'VALVE-ISOL-01'}), (b:Equipment {equipment_id: 'VALVE-OUTLET-01'}) CREATE (a)-[r:CONNECTS_TO {flow_direction: 'FORWARD', propagation_probability: 0.95, connection_type: 'FLANGE', diameter_mm: 700, length_m: 10}]->(b)");
            tx.run("MATCH (a:Equipment {equipment_id: 'PUMP-MAIN-02'}), (b:Equipment {equipment_id: 'PIPE-COLD-01'}) CREATE (a)-[r:CONNECTS_TO {flow_direction: 'FORWARD', propagation_probability: 0.95, connection_type: 'WELD', diameter_mm: 700, length_m: 25}]->(b)");
            tx.run("MATCH (a:Equipment {equipment_id: 'HX-SG-01'}), (b:Equipment {equipment_id: 'SENSOR-TEMP-02'}) CREATE (a)-[r:CONNECTS_TO {flow_direction: 'BIDIRECTIONAL', propagation_probability: 0.99, connection_type: 'INSTRUMENT', diameter_mm: 50, length_m: 1}]->(b)");

            return null;
        });

        log.info("Sample secondary loop topology inserted: 14 equipment nodes, 13 connections");
    }
}
