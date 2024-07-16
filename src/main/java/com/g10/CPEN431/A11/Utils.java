package com.g10.CPEN431.A11;

import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;
import java.util.Arrays;

public class Utils {
    static final int BUFFER_SIZE_1KB = 1024; // 32KB
    static final int BUFFER_SIZE = 32 * BUFFER_SIZE_1KB; // 32KB
    static final long TEN_POW_NINE = 1_000_000_000;

    static final int FORWARDED_MSG = 1, EPIDEMIC_MSG = 2, KV_TRANSFER_MSG = 3, REPLICA_MSG = 4;

    static final int ACK_WRONG_MSG_ID = 1, ACK_WRONG_CHECKSUM = 2, ACK_WRONG_MSG_TYPE = 3, ACK_ERROR = 4,
            ACK_SUCCESS = 0;

    public static boolean receiveAck(DatagramSocket socket)
            throws SocketTimeoutException, IOException {
        byte[] buf = new byte[Utils.BUFFER_SIZE];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);

        socket.receive(packet);
        // System.out.println("Received ack packet from " + packet.getAddress() + ":" +
        // packet.getPort());
        try {
            Message.Msg ackMsg = Message.Msg.parseFrom(Arrays.copyOf(packet.getData(), packet.getLength()));
            return processAck(ackMsg);
        } catch (InvalidProtocolBufferException e) {
            System.out.println("PrimaryReplica: InvalidProtocolBufferException!");
        }

        return false;
    }

    public static boolean processAck(Message.Msg ackMsg) {
        ByteString messageID = ackMsg.getMessageID();

        boolean areCheckSumsEqual = ackMsg.getCheckSum() == SerializeUtils.calcChecksum(messageID.toByteArray(),
                ackMsg.getPayload().toByteArray(), ackMsg.getType());
        if (!areCheckSumsEqual) {
            // System.out.println("checksums not equal");
            return false;
        }

        if (ackMsg.getType() != Utils.KV_TRANSFER_MSG) {
            return false;
        }

        try {
            KeyValueResponse.KVResponse kvResponse = KeyValueResponse.KVResponse.parseFrom(ackMsg.getPayload());

            // change number to what an ACK will be
            if (kvResponse.getErrCode() == Server.E_ACK) {
                return true;
            }
        } catch (InvalidProtocolBufferException e) {
            System.out.println("PrimaryReplica: InvalidProtocolBufferException!");
        }

        return false;
    }
}
