package com.nuclear.scada.faulttracing.config;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.neo4j.config.AbstractNeo4jConfig;
import org.springframework.data.neo4j.repository.config.EnableNeo4jRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableNeo4jRepositories(basePackages = "com.nuclear.scada.faulttracing.repository")
@EnableTransactionManagement
public class Neo4jConfig extends AbstractNeo4jConfig {

    @Value("${spring.neo4j.uri:bolt://localhost:7687}")
    private String uri;

    @Value("${spring.neo4j.authentication.username:neo4j}")
    private String username;

    @Value("${spring.neo4j.authentication.password:scada1234}")
    private String password;

    @Bean
    @Override
    public Driver neo4jDriver() {
        Map<String, String> config = new HashMap<>();
        config.put("connection.acquisition-timeout", "30s");
        config.put("connection.liveness-check-timeout", "30s");
        config.put("connection.timeout", "30s");
        config.put("max.connection.pool.size", "50");

        return GraphDatabase.driver(
                uri,
                AuthTokens.basic(username, password),
                org.neo4j.driver.Config.builder()
                        .withConnectionAcquisitionTimeout(java.time.Duration.ofSeconds(30))
                        .withConnectionLivenessCheckTimeout(java.time.Duration.ofSeconds(30))
                        .withConnectionTimeout(java.time.Duration.ofSeconds(30))
                        .withMaxConnectionPoolSize(50)
                        .build()
        );
    }
}
