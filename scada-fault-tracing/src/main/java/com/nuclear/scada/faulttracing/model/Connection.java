package com.nuclear.scada.faulttracing.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.neo4j.core.schema.GeneratedValue;
import org.springframework.data.neo4j.core.schema.Id;
import org.springframework.data.neo4j.core.schema.Property;
import org.springframework.data.neo4j.core.schema.RelationshipProperties;
import org.springframework.data.neo4j.core.schema.TargetNode;

@RelationshipProperties
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Connection {

    @Id
    @GeneratedValue
    private Long id;

    @TargetNode
    private EquipmentNode target;

    @Property(name = "flow_direction")
    private FlowDirection flowDirection;

    @Property(name = "propagation_probability")
    private double propagationProbability;

    @Property(name = "connection_type")
    private String connectionType;

    @Property(name = "diameter_mm")
    private Integer diameterMm;

    @Property(name = "length_m")
    private Integer lengthM;

    public enum FlowDirection {
        FORWARD,
        REVERSE,
        BIDIRECTIONAL
    }
}
