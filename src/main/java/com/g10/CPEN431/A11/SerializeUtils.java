package com.g10.CPEN431.A11;

import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import java.util.zip.CRC32;

public class SerializeUtils {
    public static byte[] serializeResPayload(int errorCode, Object... args) {
        KeyValueResponse.KVResponse.Builder builder = KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(errorCode);
        if (args.length > 0 && args[0] != null) {
            builder.setValue(ByteString.copyFrom((byte[]) args[0]));
        }
        if (args.length > 1 && args[1] != null) {
            builder.setPid((Integer) args[1]);
        }
        if (args.length > 2 && args[2] != null) {
            builder.setVersion((Integer) args[2]);
        }
        if (args.length > 3 && args[3] != null) {
            // builder.setOverloadWaitTime((Integer) args[3]);
            builder.setOverloadWaitTime(250);
        }
        if (args.length > 4 && args[4] != null) {
            builder.setMembershipCount((Integer) args[4]);
        }
        KeyValueResponse.KVResponse resPayload = builder.build();
        return resPayload.toByteArray();
    }

    public static byte[] serializeMessage(byte[] messageID, byte[] reqPayload) {
        Message.Msg.Builder builder = Message.Msg.newBuilder()
                .setMessageID(ByteString.copyFrom(messageID))
                .setPayload(ByteString.copyFrom(reqPayload))
                .setCheckSum(calcChecksum(messageID, reqPayload));
        Message.Msg msg = builder.build();
        return msg.toByteArray();
    }

    public static byte[] serializeMessage(byte[] messageID, byte[] reqPayload, int type) {
        Message.Msg.Builder builder = Message.Msg.newBuilder()
                .setMessageID(ByteString.copyFrom(messageID))
                .setPayload(ByteString.copyFrom(reqPayload))
                .setCheckSum(calcChecksum(messageID, reqPayload, type))
                .setType(type);
        Message.Msg msg = builder.build();
        return msg.toByteArray();
    }

    public static long calcChecksum(byte[] messageID, byte[] payload) {
        CRC32 crc32 = new CRC32();
        byte[] concatArr = Arrays.copyOf(messageID, messageID.length + payload.length);
        System.arraycopy(payload, 0, concatArr, messageID.length, payload.length);
        crc32.update(concatArr);
        return crc32.getValue();
    }

    public static long calcChecksum(byte[] messageID, byte[] payload, int type) {
        CRC32 crc32 = new CRC32();
        byte[] concatArr = Arrays.copyOf(messageID, messageID.length + payload.length);
        System.arraycopy(payload, 0, concatArr, messageID.length, payload.length);
        crc32.update(concatArr);
        crc32.update(type);
        return crc32.getValue();
    }

    // Used ChatGPT
    public static byte[] generateMessageID() {
        UUID uuid = UUID.randomUUID();
        long time = System.currentTimeMillis();
        byte[] messageID = new byte[16];
        byte[] uuidBytes = ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits()).array();
        byte[] timeBytes = ByteBuffer.allocate(8).putLong(time).array();
        System.arraycopy(uuidBytes, 0, messageID, 0, 16);
        System.arraycopy(timeBytes, 0, messageID, 0, 8);
        return messageID;
    }
}
