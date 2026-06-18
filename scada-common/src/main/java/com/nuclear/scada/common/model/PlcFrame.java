package com.nuclear.scada.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PlcFrame {

    private String plcId;
    private int frameType;
    private int sequenceNumber;
    private int payloadLength;
    private byte[] payload;
    private Instant receivedAt;

    public static final int FRAME_HEADER_SIZE = 24;
    public static final byte MAGIC_BYTE_1 = (byte) 0x46;
    public static final byte MAGIC_BYTE_2 = (byte) 0x53;
}
