package com.nuclear.scada.netty;

import com.nuclear.scada.codec.OpcUaBinaryDecoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import javax.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScadaNettyServer {

    @Value("${scada.netty.port:4840}")
    private int port;

    @Value("${scada.netty.boss-threads:2}")
    private int bossThreads;

    @Value("${scada.netty.worker-threads:8}")
    private int workerThreads;

    @Value("${scada.netty.idle-timeout-seconds:120}")
    private int idleTimeoutSeconds;

    private final PlcDataHandler plcDataHandler;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ChannelFuture channelFuture;

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(bossThreads);
        workerGroup = new NioEventLoopGroup(workerThreads);

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast("idleState", new IdleStateHandler(idleTimeoutSeconds, 0, 0, TimeUnit.SECONDS))
                                .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
                                        65536, 20, 4, 0, 0))
                                .addLast("opcUaDecoder", new OpcUaBinaryDecoder())
                                .addLast("plcHandler", plcDataHandler);
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_RCVBUF, 256 * 1024)
                .childOption(ChannelOption.SO_SNDBUF, 64 * 1024);

        channelFuture = bootstrap.bind(port).sync();
        log.info("SCADA Netty server started on port {}, waiting for PLC connections...", port);
    }

    @PreDestroy
    public void stop() {
        log.info("Shutting down SCADA Netty server...");
        if (channelFuture != null) {
            channelFuture.channel().close().syncUninterruptibly();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        }
        log.info("SCADA Netty server stopped.");
    }
}
