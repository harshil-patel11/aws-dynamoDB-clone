package com.g10.CPEN431.A11;

import ca.NetSysLab.ProtocolBuffers.EpidemicPayload;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class Epidemic implements Runnable {
    private Router router;
    private int serverNodeId;
    private DatagramSocket socket;
    private Object socketLock;
    private int numNodesChosenForEpidemic;
    private int nodeExpirationTime;
    private KVStore kvStore;

    private Node serverNode;
    private Object setIsAliveNodeLock;

    public Epidemic(Router router, int serverNodeId, DatagramSocket socket, Object socketLock, KVStore kvStore,
            int numNodesChosenForEpidemic, int nodeExpirationTime, Object setIsAliveNodeLock) {
        this.router = router;
        this.serverNodeId = serverNodeId;
        this.socket = socket;
        this.socketLock = socketLock;
        this.numNodesChosenForEpidemic = numNodesChosenForEpidemic;
        this.nodeExpirationTime = nodeExpirationTime;
        this.kvStore = kvStore;
        this.serverNode = this.router.getNodeById(serverNodeId);
        this.setIsAliveNodeLock = setIsAliveNodeLock;
    }

    @Override
    public void run() {
        try {
            DatagramPacket isAlivePacket;
            byte[] isAliveBuf;
            Collection<Node> nodes = router.getRandomNodes(numNodesChosenForEpidemic, serverNodeId, serverNodeId)
                    .values();

            byte[] messageID = SerializeUtils.generateMessageID();
            EpidemicPayload.EpidemicPld ePayload = EpidemicPayload.EpidemicPld
                    .newBuilder()
                    .setNodeID(serverNodeId)
                    .setTimestamp(System.nanoTime())
                    .setPropagationProbability(1.0)
                    .build();

            isAliveBuf = SerializeUtils.serializeMessage(messageID, ePayload.toByteArray(), Utils.EPIDEMIC_MSG);

            // check itself if it is joining
            Server.updateIsAliveTimestamp(kvStore);

            for (Node node : nodes) {
                try {
                    isAlivePacket = new DatagramPacket(isAliveBuf, isAliveBuf.length, node.getIP(), node.getPort());
                    synchronized (socketLock) {
                        this.socket.send(isAlivePacket);
                    }
                } catch (IOException e) {
                    System.out.println(e.getStackTrace());
                }
            }

            // check for dead nodes
            for (Node node : router.getNodes()) {
                // skip the node itself
                if (serverNodeId == node.getNodeId()) {
                    continue;
                }
                synchronized (setIsAliveNodeLock) {
                    if (!node.getIsAlive()) {
                        continue;
                    }
                    long currentTime = System.nanoTime() / Utils.TEN_POW_NINE;
                    long timestamp = node.getLastLocalTimestamp() / Utils.TEN_POW_NINE;

                    if (currentTime - timestamp > nodeExpirationTime) {
                        router.invalidateRoute(node.getNodeId());
                        System.out.println("Node " + node.getNodeId() + " is dead!!!");

                        long curTime = System.currentTimeMillis();
                        synchronized (Server.isAliveLock) {
                            if (curTime - Server.isAliveTimestamp < Server.NO_TRANSFER_PERIOD) {
                                System.out.println(
                                        "Cannot run MaintainReplicas. Node " + serverNodeId + " came back alive."
                                                + " curTime: " + curTime);
                                continue;
                            }
                        }
                        System.out
                                .println("Maintaining Replicas for: Node " + node.getNodeId() + " curTime: " + curTime);

                        List<Node> serverOwnedNodes = router.getOwnedNodes(serverNode);
                        List<Node> newlyOwnedNodes = serverNode.getNewlyOwnedNodes(node,
                                serverOwnedNodes);

                        if (newlyOwnedNodes != null) {
                            System.out.println("Printing newly owned nodes");
                            for (Node newNode : newlyOwnedNodes) {
                                System.out.println("Newly Owned Node: " + newNode.getNodeId());
                            }

                            List<Node> serverReplicas = serverNode.getReplicas();

                            for (Node replicaNode : serverReplicas) {
                                // System.out.println("New Replicas for leaving node: " +
                                // replicaNode.getNodeId());
                                MaintainReplicas maintainReplicas = new MaintainReplicas(kvStore, newlyOwnedNodes,
                                        replicaNode);
                                Thread maintainReplicasThread = new Thread(maintainReplicas);
                                maintainReplicasThread.start();
                            }
                        } else {
                            Node targetNode = serverNode.updateReplicaStateNodeLeave(node, serverOwnedNodes);

                            if (targetNode != null) {
                                MaintainReplicas maintainReplicas = new MaintainReplicas(kvStore, serverOwnedNodes,
                                        targetNode);
                                Thread maintainReplicasThread = new Thread(maintainReplicas);
                                maintainReplicasThread.start();
                            }
                        }

                    }
                }
            }
        } catch (Exception e) {
            System.out.println(Arrays.toString(e.getStackTrace()));
        }
    }
}
