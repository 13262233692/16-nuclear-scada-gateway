package com.nuclear.scada;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class NuclearScadaGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(NuclearScadaGatewayApplication.class, args);
    }
}
