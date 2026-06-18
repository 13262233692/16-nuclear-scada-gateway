package com.nuclear.scada.netty;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Slf4j
@Component
@RequiredArgsConstructor
public class NettyServerRunner {

    private final ScadaNettyServer nettyServer;

    @PostConstruct
    public void init() {
        Thread serverThread = new Thread(() -> {
            try {
                nettyServer.start();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Netty server interrupted", e);
            } catch (Exception e) {
                log.error("Netty server startup failed", e);
            }
        }, "netty-scada-server");
        serverThread.setDaemon(true);
        serverThread.start();
    }
}
