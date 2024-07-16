package com.g10.CPEN431.A11;

import ca.NetSysLab.ProtocolBuffers.KeyValueTransfer;
import com.g10.CPEN431.A11.KVStore.Value;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TransferNodeData implements Runnable {
    private DatagramSocket socket;
    private KVStore kvStore;
    private Router router;
    private Node joiningNode;

    public TransferNodeData(KVStore kvStore, Router router, Node joiningNode, Object lock) throws SocketException {
        this.socket = new DatagramSocket();
        this.kvStore = kvStore;
        this.router = router;
        this.joiningNode = joiningNode;
    }

    @Override
    public void run() {
        try {
            List<Integer> bucketIds = getBucketIdsToTransfer();
            System.out.println("TransferNodeData: Bucket IDs to transfer: ");
            for (int bucketId : bucketIds) {
                System.out.println("Bucket ID: " + bucketId);
            }

            for (int bucketId : bucketIds) {
                Set<ByteString> keys = kvStore.getKeys(bucketId)
                        .stream()
                        .map((ByteString originalKey) -> ByteString.copyFrom(originalKey.toByteArray()))
                        .collect(Collectors.toSet());
                System.out.println("TransferNodeData: Bucket " + bucketId + " has " +
                        keys.size() + " keys");
                        
                long count = 0;
                for (ByteString key : keys) {
                    Value value = kvStore.getInternal(key, bucketId);
                    if (value == null) {
                        // System.out
                        // .println("TransferNodeData: Key " + Arrays.toString(key.toByteArray())
                        // + " not found in bucket "
                        // + bucketId);
                        continue;
                    }
                    count++;

                    byte[] messageID = SerializeUtils.generateMessageID();
                    KeyValueTransfer.KVTransfer kvTransfer = KeyValueTransfer.KVTransfer
                            .newBuilder()
                            .setCommand(Server.PUT)
                            .setKey(key)
                            .setValue(value.getValue() == null ? ByteString.copyFrom(new byte[0])
                                    : ByteString.copyFrom(value.getValue()))
                            .setVersion(value.getVersion())
                            .setLpClock(value.getLpClock())
                            .build();

                    byte[] kvTransferMsg = SerializeUtils.serializeMessage(messageID, kvTransfer.toByteArray(),
                            Utils.KV_TRANSFER_MSG);

                    // System.out.println("TransferNodeData: Transferring key with version: " +
                    // value.getVersion() + " key: " + Arrays.toString(key.toByteArray()) + " to
                    // node " +
                    // joiningNode.getNodeId());

                    DatagramPacket kvTransferPacket = new DatagramPacket(kvTransferMsg,
                            kvTransferMsg.length,
                            joiningNode.getIP(), joiningNode.getPort() + 1);

                    try {
                        socket.send(kvTransferPacket);
                        socket.send(kvTransferPacket);
                    } catch (IOException e) {
                        System.out
                                .println("TransferNodeData: Error sending key to node " + joiningNode.getNodeId());
                        e.printStackTrace();
                    }                   
                }
                System.out.println(
                        "TransferNodeData: Transferred " + count + " keys from bucket " + bucketId +
                                " to node "
                                + joiningNode.getNodeId());
            }
        } catch (Throwable e) {
            System.out.println("TransferNodeData: Error transferring data to node " + joiningNode.getNodeId());
            e.printStackTrace();
        }

        this.socket.close();
    }

    private List<Integer> getBucketIdsToTransfer() {
        List<Integer> bucketIds = new ArrayList<>();

        Node joiningNodePredecessor = joiningNode.getPredecessor();
        int nextNodeId = (joiningNodePredecessor.getNodeId() + 1) % router.getNumNodes();

        while (nextNodeId != joiningNode.getNodeId()) {
            bucketIds.add(nextNodeId);
            nextNodeId = (nextNodeId + 1) % router.getNumNodes();
        }

        bucketIds.add(joiningNode.getNodeId());

        return bucketIds;
    }

    public static boolean isNodeDataTransferRequired(KVStore kvStore, Router router, Node serverNode,
            Node joiningNode) {
        if (joiningNode.getNodeId() == serverNode.getNodeId()) {
            return false;
        }

        if (serverNode.getPredecessor().getNodeId() == joiningNode.getNodeId()) {
            return true;
        }

        Node joiningNodePredecessor = joiningNode.getPredecessor();
        int nextNodeId = (joiningNodePredecessor.getNodeId() + 1) % router.getNumNodes();

        while (nextNodeId != joiningNode.getNodeId()) {
            if (kvStore.getKeys(nextNodeId).size() > 0) {
                return true;
            }
            nextNodeId = (nextNodeId + 1) % router.getNumNodes();
        }

        if (kvStore.getKeys(joiningNode.getNodeId()).size() > 0) {
            return true;
        }

        return false;
    }
}
