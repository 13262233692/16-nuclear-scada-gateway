package com.nuclear.scada.codec;

import com.nuclear.scada.common.model.PlcFrame;
import com.nuclear.scada.common.model.SensorData;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class OpcUaBinaryDecoder extends ByteToMessageDecoder {

    private static final int MIN_FRAME_SIZE = PlcFrame.FRAME_HEADER_SIZE + 4;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        while (in.readableBytes() >= MIN_FRAME_SIZE) {
            in.markReaderIndex();

            byte magic1 = in.readByte();
            byte magic2 = in.readByte();

            if (magic1 != PlcFrame.MAGIC_BYTE_1 || magic2 != PlcFrame.MAGIC_BYTE_2) {
                in.resetReaderIndex();
                in.skipBytes(1);
                continue;
            }

            int frameType = in.readUnsignedByte();
            int version = in.readUnsignedByte();
            int sequenceNumber = in.readShort();
            int headerReserved = in.readShort();
            long epochMillis = in.readLong();
            int payloadLength = in.readInt();

            if (in.readableBytes() < payloadLength) {
                in.resetReaderIndex();
                return;
            }

            byte[] plcIdBytes = new byte[16];
            in.readBytes(plcIdBytes);
            String plcId = new String(plcIdBytes, StandardCharsets.US_ASCII).trim();

            int sensorPayloadLen = payloadLength - 16;
            if (sensorPayloadLen <= 0) {
                continue;
            }

            byte[] sensorPayload = new byte[sensorPayloadLen];
            in.readBytes(sensorPayload);

            PlcFrame frame = PlcFrame.builder()
                    .plcId(plcId)
                    .frameType(frameType)
                    .sequenceNumber(sequenceNumber)
                    .payloadLength(payloadLength)
                    .payload(sensorPayload)
                    .receivedAt(Instant.ofEpochMilli(epochMillis))
                    .build();

            List<SensorData> sensorDataList = decodeSensorPayload(frame);
            out.add(sensorDataList);
        }
    }

    private List<SensorData> decodeSensorPayload(PlcFrame frame) {
        List<SensorData> result = new ArrayList<>();
        byte[] payload = frame.getPayload();
        int offset = 0;

        while (offset + 4 <= payload.length) {
            int tagId = ((payload[offset] & 0xFF) << 8) | (payload[offset + 1] & 0xFF);
            int dataLen = ((payload[offset + 2] & 0xFF) << 8) | (payload[offset + 3] & 0xFF);
            offset += 4;

            if (offset + dataLen > payload.length) {
                log.warn("Truncated sensor payload at offset {}, declared len={}, remaining={}",
                        offset, dataLen, payload.length - offset);
                break;
            }

            SensorData.SensorType sensorType = mapTagToSensorType(tagId);
            if (sensorType == null) {
                offset += dataLen;
                continue;
            }

            SensorData data = parseSensorData(tagId, sensorType, payload, offset, dataLen, frame);
            if (data != null) {
                result.add(data);
            }

            offset += dataLen;
        }

        return result;
    }

    private SensorData parseSensorData(int tagId, SensorData.SensorType type,
                                        byte[] payload, int offset, int len, PlcFrame frame) {
        if (len < 6) return null;

        int nodeIdLen = payload[offset] & 0xFF;
        offset++;
        int qualityCode = payload[offset] & 0xFF;
        offset++;

        String nodeId;
        if (nodeIdLen > 0 && offset + nodeIdLen <= payload.length) {
            nodeId = new String(payload, offset, nodeIdLen, StandardCharsets.US_ASCII);
            offset += nodeIdLen;
        } else {
            nodeId = "ns=2;s=" + tagId;
        }

        double value;
        int valueBytesRemaining = len - 2 - nodeIdLen;
        if (valueBytesRemaining >= 8) {
            value = Double.longBitsToDouble(readLongLE(payload, offset));
        } else if (valueBytesRemaining >= 4) {
            value = Float.intBitsToFloat(readIntLE(payload, offset));
        } else if (valueBytesRemaining >= 2) {
            value = readShortLE(payload, offset);
        } else {
            value = 0.0;
        }

        String unit = resolveUnit(type);

        return SensorData.builder()
                .plcId(frame.getPlcId())
                .nodeId(nodeId)
                .sensorType(type)
                .value(value)
                .unit(unit)
                .timestamp(frame.getReceivedAt())
                .qualityCode(SensorData.QualityCode.fromCode(qualityCode))
                .build();
    }

    private SensorData.SensorType mapTagToSensorType(int tagId) {
        switch (tagId) {
            case 0x1001:
            case 0x1002:
            case 0x1003:
                return SensorData.SensorType.COOLANT_PRESSURE;
            case 0x2001:
            case 0x2002:
            case 0x2003:
                return SensorData.SensorType.MAIN_PUMP_SPEED;
            case 0x3001:
            case 0x3002:
            case 0x3003:
            case 0x3004:
            case 0x3005:
                return SensorData.SensorType.EXTREME_TEMPERATURE;
            default:
                return null;
        }
    }

    private String resolveUnit(SensorData.SensorType type) {
        switch (type) {
            case COOLANT_PRESSURE:
                return "MPa";
            case MAIN_PUMP_SPEED:
                return "RPM";
            case EXTREME_TEMPERATURE:
                return "°C";
            default:
                return "";
        }
    }

    private static int readIntLE(byte[] buf, int off) {
        return (buf[off] & 0xFF)
                | ((buf[off + 1] & 0xFF) << 8)
                | ((buf[off + 2] & 0xFF) << 16)
                | ((buf[off + 3] & 0xFF) << 24);
    }

    private static long readLongLE(byte[] buf, int off) {
        return (buf[off] & 0xFFL)
                | ((buf[off + 1] & 0xFFL) << 8)
                | ((buf[off + 2] & 0xFFL) << 16)
                | ((buf[off + 3] & 0xFFL) << 24)
                | ((buf[off + 4] & 0xFFL) << 32)
                | ((buf[off + 5] & 0xFFL) << 40)
                | ((buf[off + 6] & 0xFFL) << 48)
                | ((buf[off + 7] & 0xFFL) << 56);
    }

    private static short readShortLE(byte[] buf, int off) {
        return (short) ((buf[off] & 0xFF) | ((buf[off + 1] & 0xFF) << 8));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("OPC-UA binary decode error from PLC channel {}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}
