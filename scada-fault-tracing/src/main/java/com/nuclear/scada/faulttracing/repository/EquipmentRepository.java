package com.nuclear.scada.faulttracing.repository;

import com.nuclear.scada.faulttracing.model.EquipmentNode;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.data.neo4j.repository.query.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface EquipmentRepository extends Neo4jRepository<EquipmentNode, Long> {

    Optional<EquipmentNode> findByEquipmentId(String equipmentId);

    List<EquipmentNode> findByEquipmentType(EquipmentNode.EquipmentType type);

    List<EquipmentNode> findByPlcId(String plcId);

    @Query("MATCH (n:Equipment {equipment_id: $equipmentId}) " +
           "MATCH path = (n)-[r:CONNECTS_TO*1..5]->(upstream:Equipment) " +
           "WHERE ALL(rel IN r WHERE rel.flow_direction IN ['FORWARD', 'BIDIRECTIONAL']) " +
           "RETURN nodes(path), relationships(path) " +
           "ORDER BY length(path) ASC")
    List<EquipmentNode> findUpstreamEquipment(@Param("equipmentId") String equipmentId);

    @Query("MATCH (n:Equipment {equipment_id: $equipmentId}) " +
           "MATCH (upstream:Equipment)-[r:CONNECTS_TO*1..5]->(n) " +
           "WHERE ALL(rel IN r WHERE rel.flow_direction IN ['FORWARD', 'BIDIRECTIONAL']) " +
           "RETURN DISTINCT upstream " +
           "ORDER BY id(upstream)")
    List<EquipmentNode> findDirectUpstreamEquipment(@Param("equipmentId") String equipmentId);

    @Query("MATCH (e:Equipment) DETACH DELETE e")
    void deleteAllEquipment();

    @Query("MATCH (e:Equipment {equipment_id: $fromId})-[r:CONNECTS_TO]->(t:Equipment {equipment_id: $toId}) " +
           "SET r.propagation_probability = $probability, " +
           "    r.flow_direction = $direction " +
           "RETURN count(r)")
    int updateConnectionProperties(@Param("fromId") String fromId,
                                   @Param("toId") String toId,
                                   @Param("probability") double probability,
                                   @Param("direction") String direction);

    @Query("MATCH (n:Equipment {equipment_id: $equipmentId}) " +
           "OPTIONAL MATCH (n)-[out:CONNECTS_TO]->() " +
           "OPTIONAL MATCH ()-[in:CONNECTS_TO]->(n) " +
           "RETURN count(DISTINCT out) + count(DISTINCT in) as connectionCount")
    long countConnections(@Param("equipmentId") String equipmentId);
}
