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

public class TransferReplicaData implements Runnable {
    private KVStore kvStore;
    private Router router;
    private Node joiningNode;
    private DatagramSocket socket;

    public TransferReplicaData(KVStore kvStore, Router router, Node joiningNode)
            throws SocketException {
        this.kvStore = kvStore;
        this.router = router;
        this.joiningNode = joiningNode;
        this.socket = new DatagramSocket();
    }

    @Override
    public void run() {
        try {
            // handles node joins
            synchronized (joiningNode.getExternalLock()) {
                List<Integer> bucketIds = getBucketIdsToTransfer();

                for (int bucketId : bucketIds) {

                    System.out
                    .println("TransferReplicaData: Transferring replica data from bucket " +
                    bucketId
                    + " to node "
                    + joiningNode.getNodeId());

                    Set<ByteString> keys = kvStore.getKeys(bucketId)
                            .stream()
                            .map((ByteString originalKey) -> ByteString.copyFrom(originalKey.toByteArray()))
                            .collect(Collectors.toSet());

                    for (ByteString key : keys) {
                        Value value = kvStore.getInternal(key, bucketId);
                        if (value == null) {
                            // System.out
                            // .println("TransferReplicaData: Key " + Arrays.toString(key.toByteArray())
                            // + " not found in bucket "
                            // + bucketId);
                            continue;
                        }

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
                                joiningNode.getIP(), joiningNode.getPort() + 1);
                        
                        try {
                            socket.send(kvTransferPacket);
                            socket.send(kvTransferPacket);
                            break;
                        } catch (IOException e) {
                            System.out.println("TransferReplicaData: Error sending key to node "
                                    + joiningNode.getNodeId());
                            e.printStackTrace();
                        }
                        
                    }
                }
            }
        } catch (Throwable e) {
            System.out.println(
                    "TransferReplicaData: Error transferring data to node " + joiningNode.getNodeId());
            e.printStackTrace();
        }

        this.socket.close();
    }

    private List<Integer> getBucketIdsToTransfer() {
        List<Integer> bucketIds = new ArrayList<>();

        // find bucket ids that the joining node is responsible for
        int joiningNodeId = joiningNode.getNodeId();
        for (Node node : router.getNodes()) {
            if (node.getReplica(joiningNodeId) != null) {
                if (kvStore.getKeys(node.getNodeId()).size() > 0) {
                    bucketIds.add(node.getNodeId());
                }
            }
        }

        // int replicaNodeId = joiningNodeId;
        // for (int i = 1; i <= 3; i++) {
        // replicaNodeId--;
        // if (replicaNodeId < 0) {
        // replicaNodeId = router.getNumNodes() - 1;
        // }
        // // int replicaNodeId = (joiningNodeId - i) % router.getNumNodes();
        // bucketIds.add(replicaNodeId);
        // }

        return bucketIds;
    }

    public static boolean isReplicaTransferRequired(KVStore kvStore, Router router, Node serverNode, Node joiningNode) {
        List<Node> replicaNodes = serverNode.getReplicas();
        for (Node replicaNode : replicaNodes) {
            if (replicaNode.getNodeId() == joiningNode.getNodeId()) {
                return true;
            }
        }

        // get the node ids for the 3 replicas that the joining node is responsible for
        int joiningNodeId = joiningNode.getNodeId();

        for (Node node : router.getNodes()) {
            if (node.getReplica(joiningNodeId) != null) {
                if (kvStore.getKeys(node.getNodeId()).size() > 0) {
                    return true;
                }
            }
        }

        // int replicaNodeId = joiningNodeId;
        // for (int i = 1; i <= 3; i++) {
        // replicaNodeId--;
        // if (replicaNodeId < 0) {
        // replicaNodeId = router.getNumNodes() - 1;
        // }

        // // int replicaNodeId = (joiningNodeId - i) % router.getNumNodes();
        // if (kvStore.getKeys(replicaNodeId).size() > 0) {
        // return true;
        // }
        // }

        return false;
    }

}
