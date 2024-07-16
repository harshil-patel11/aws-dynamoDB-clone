package com.g10.CPEN431.A11;

import ca.NetSysLab.ProtocolBuffers.KeyValueTransfer;
import com.g10.CPEN431.A11.KVStore.Value;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class MaintainReplicas implements Runnable {
    private KVStore kvStore;
    private Node targetNode;
    private List<Node> ownedNodes;
    private DatagramSocket socket;

    public MaintainReplicas(KVStore kvStore, List<Node> ownedNodes, Node targetNode) throws SocketException {
        this.kvStore = kvStore;
        this.ownedNodes = ownedNodes;
        this.targetNode = targetNode;
        this.socket = new DatagramSocket();
    }

    @Override
    public void run() {
        synchronized (targetNode.getExternalLock()) {
            for (Node node : ownedNodes) {
                int bucketId = node.getNodeId();
                if (bucketId == targetNode.getNodeId()) {
                    // this should never happen
                    System.out.println(
                            "MaintainReplicas: SHOULD NEVER HAPPEN. Skipping bucket ID " + bucketId + " of node "
                                    + targetNode.getNodeId());
                    continue;
                }

                System.out.println("MaintainReplicas: Transferring replica data from bucket "
                        +
                        bucketId
                        + " to node "
                        + targetNode.getNodeId());

                Set<ByteString> keys = kvStore.getKeys(bucketId)
                        .stream()
                        .map((ByteString originalKey) -> ByteString.copyFrom(originalKey.toByteArray()))
                        .collect(Collectors.toSet());
                System.out.println("MaintainReplicas: Bucket " + bucketId + " has " +
                        keys.size() + " keys");

                long count = 0;
                for (ByteString key : keys) {
                    Value value = kvStore.getInternal(key, bucketId);
                    if (value == null) {
                        // System.out
                        // .println("TransferReplicaData: Key " + Arrays.toString(key.toByteArray())
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

                    DatagramPacket kvTransferPacket = new DatagramPacket(kvTransferMsg, kvTransferMsg.length,
                            targetNode.getIP(), targetNode.getPort() + 1);

                    try {
                        socket.send(kvTransferPacket);
                        socket.send(kvTransferPacket);
                    } catch (IOException e) {
                        System.out.println("MaintainReplicas: Error sending key to node "
                                + targetNode.getNodeId());
                        e.printStackTrace();
                    }
                }
                System.out.println(
                        "MaintainReplicas: Transferred " + count + " keys from bucket " + bucketId +
                                " to node "
                                + targetNode.getNodeId());
            }
        }

        this.socket.close();
    }

}
