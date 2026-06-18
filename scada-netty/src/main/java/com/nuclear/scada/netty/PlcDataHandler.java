package com.nuclear.scada.netty;

import com.nuclear.scada.common.model.SensorData;
import com.nuclear.scada.kafka.SensorKafkaProducer;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@ChannelHandler.Sharable
@RequiredArgsConstructor
public class PlcDataHandler extends SimpleChannelInboundHandler<List<SensorData>> {

    private final SensorKafkaProducer kafkaProducer;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, List<SensorData> sensorDataList) {
        if (sensorDataList == null || sensorDataList.isEmpty()) return;
        for (SensorData data : sensorDataList) {
            kafkaProducer.publish(data);
        }
        log.debug("Processed {} sensor readings from PLC channel {}", sensorDataList.size(), ctx.channel().remoteAddress());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("PLC data handler error for channel {}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.warn("PLC connection lost: {}", ctx.channel().remoteAddress());
        ctx.fireChannelInactive();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.info("PLC connected: {}", ctx.channel().remoteAddress());
        ctx.fireChannelActive();
    }
}
