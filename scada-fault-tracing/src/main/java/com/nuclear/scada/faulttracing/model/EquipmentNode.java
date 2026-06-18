package com.nuclear.scada.faulttracing.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.neo4j.core.schema.GeneratedValue;
import org.springframework.data.neo4j.core.schema.Id;
import org.springframework.data.neo4j.core.schema.Node;
import org.springframework.data.neo4j.core.schema.Property;
import org.springframework.data.neo4j.core.schema.Relationship;

import java.util.ArrayList;
import java.util.List;

@Node(labels = {"Equipment"})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EquipmentNode {

    @Id
    @GeneratedValue
    private Long id;

    @Property(name = "equipment_id")
    private String equipmentId;

    @Property(name = "equipment_type")
    private EquipmentType equipmentType;

    @Property(name = "name")
    private String name;

    @Property(name = "location")
    private String location;

    @Property(name = "plc_id")
    private String plcId;

    @Property(name = "node_id")
    private String nodeId;

    @Property(name = "criticality")
    private double criticality;

    @Property(name = "status")
    private String status;

    @Relationship(type = "CONNECTS_TO", direction = Relationship.Direction.OUTGOING)
    private List<Connection> outgoingConnections = new ArrayList<>();

    @Relationship(type = "CONNECTS_TO", direction = Relationship.Direction.INCOMING)
    private List<Connection> incomingConnections = new ArrayList<>();

    public enum EquipmentType {
        VALVE,       // 阀门
        PIPE,        // 管道
        PUMP,        // 水泵
        HEAT_EXCHANGER,
        SENSOR,
        OTHER
    }
}
