package com.g10.CPEN431.A11;

import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;

public class Node {
    private InetAddress ip;
    private int port;
    private int nodeId;
    private boolean isAlive;
    private BigInteger minKey;
    private BigInteger maxKey;
    private ByteString lastIsAliveMessageId;
    private long lastIsAliveMessageTimestamp;
    private long lastLocalTimestamp;
    final private BigInteger lowerBound;
    final private BigInteger upperBound;

    private Node successor;
    private Node predecessor;
    // private List<Node> replicas;

    private Object lock;
    private Object externalLock;
    // store last isAlive message Id
    // last isAlive message received timestamp (record this timestamp based on the
    // node's local time, not the sender's time)
    // if message id is same as before, ignore updating the state

    public Node(int nodeId, InetAddress ip, int port, boolean isAlive, BigInteger minKey, BigInteger maxKey) {
        this.nodeId = nodeId;
        this.ip = ip;
        this.port = port;
        this.isAlive = isAlive;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.lowerBound = minKey;
        this.upperBound = maxKey;

        this.lastIsAliveMessageId = null;
        this.lastIsAliveMessageTimestamp = 0;
        this.lastLocalTimestamp = System.nanoTime();

        this.successor = null;
        this.predecessor = null;
        // this.replicas = new ArrayList<>();
        this.lock = new Object();
        this.externalLock = new Object();
    }

    // NOTE: can be optimized by transferring only the newly node we own to the new
    // replica if we own the leaving node
    public Node updateReplicaStateNodeLeave(Node leavingNode, List<Node> ownedNodes) {
        System.out.println("updateReplicaStateNodeLeave: Node " + leavingNode.getNodeId() + " left");
        System.out.println("updateReplicaStateNodeLeave: printing owned nodes");
        for (Node node : ownedNodes) {
            System.out.println("updateReplicaStateNodeLeave: Owned Node: " + node.getNodeId() + " Replica Length: "
                    + node.getReplicas().size());
        }

        Node newReplica = this.successor.getSuccessor().getSuccessor();
        if (ownedNodes.contains(leavingNode)) {
            return newReplica;
        } else {
            int newReplicaNodeId = newReplica.getNodeId();
            int leavingNodeId = leavingNode.getNodeId();

            // check if leaving node id is between the new replica and the current node but
            // account for wrap around
            if (newReplicaNodeId > this.nodeId) {
                if (this.nodeId < leavingNodeId && leavingNodeId < newReplicaNodeId) {
                    System.out.println("updateReplicaStateNodeLeave: replica Node " + newReplicaNodeId + " joined");
                    return newReplica;
                }
            } else {
                if (leavingNodeId < newReplicaNodeId || leavingNodeId > this.nodeId) {
                    System.out.println("updateReplicaStateNodeLeave: wrap around case");
                    System.out.println("updateReplicaStateNodeLeave: replica Node " + newReplicaNodeId + " joined");
                    return newReplica;
                }
            }

            return null;
        }
    }
    // public Node updateReplicaStateNodeLeave(Node leavingNode, List<Node>
    // ownedNodes) {
    // // find nodes we own
    // if (ownedNodes.contains(leavingNode)) {
    // Node replicaNodeToRemove = null;
    // for (Node replica : leavingNode.getReplicas()) {
    // if (replica.getNodeId() == this.nodeId) {
    // replicaNodeToRemove = replica;
    // break;
    // }
    // }

    // // remove the replica node from the leaving node
    // if (replicaNodeToRemove != null) {
    // leavingNode.removeReplica(replicaNodeToRemove);
    // } // else {
    // // System.out.println("updateReplicaState: replicaNodeToRemove is null!!");
    // // }

    // Node newReplica = this.successor.getSuccessor().getSuccessor();
    // // System.out.println("updateReplicaState: replica Node " + newReplica.nodeId
    // // + " joined, new node is owned by the current node");
    // leavingNode.addReplica(newReplica);
    // return newReplica;
    // } else {
    // Node newReplica = this.successor.getSuccessor().getSuccessor();
    // // System.out.println("updateReplicaState: replica Node " + newReplica.nodeId
    // // + " joined, new node is NOT owned by the current node");
    // boolean newReplicaAdded = false;
    // for (Node node : ownedNodes) {
    // boolean wasRemoved = node.removeReplica(leavingNode);
    // if (wasRemoved) {
    // // System.out.println("Removed replica from node: " + node.getNodeId());
    // node.addReplica(newReplica);
    // newReplicaAdded = true;
    // }
    // // System.out.println("Owned Node: " + node.getNodeId());
    // }

    // // for (Node node : ownedNodes) {
    // // System.out.println("updateReplicaState: Owned Node: " + node.getNodeId() +
    // "
    // // Replica Length: "
    // // + node.getReplicas().size());
    // // }

    // if (newReplicaAdded) {
    // return newReplica;
    // } else {
    // // System.out.println("updateReplicaState: newReplica not added to any owned
    // // node");
    // }
    // return null;
    // }
    // }

    /**
     * Check if the node that left join "this" key space
     * 
     * @return List<Node> nodes to be replicated
     */
    public List<Node> getNewlyOwnedNodes(Node leavingNode, List<Node> ownedNodes) {
        if (ownedNodes.contains(leavingNode)) {
            List<Node> newOwnedNodes = new ArrayList<>();
            for (Node node : ownedNodes) {
                System.out.println("getNewlyOwnedNodes: Owned Node: " + node.getNodeId());
                newOwnedNodes.add(node);
                if (node == leavingNode) {
                    return newOwnedNodes;
                }
            }
        }
        return null;
    }

    public String getAddress() {
        return ip.getHostAddress() + ":" + port;
    }

    public InetAddress getIP() {
        return ip;
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getPort() {
        return port;
    }

    public boolean getIsAlive() {
        return isAlive;
    }

    public void setIsAlive(boolean isAlive) {
        synchronized (lock) {
            this.isAlive = isAlive;
        }
    }

    public BigInteger getMinKey() {
        return minKey;
    }

    public void setMinKey(BigInteger minKey) {
        synchronized (lock) {
            this.minKey = minKey;
        }
    }

    public BigInteger getMaxKey() {
        return maxKey;
    }

    public void setMaxKey(BigInteger maxKey) {
        synchronized (lock) {
            this.maxKey = maxKey;
        }
    }

    public ByteString getLastIsAliveMessageId() {
        return lastIsAliveMessageId;
    }

    public void setLastIsAliveMessageId(ByteString lastIsAliveMessageId) {
        synchronized (lock) {
            this.lastIsAliveMessageId = lastIsAliveMessageId;
        }
    }

    public long getLastIsAliveMessageTimestamp() {
        return lastIsAliveMessageTimestamp;
    }

    public void setLastIsAliveMessageTimestamp(long lastIsAliveMessageTimestamp) {
        synchronized (lock) {
            this.lastIsAliveMessageTimestamp = lastIsAliveMessageTimestamp;
        }
    }

    public long getLastLocalTimestamp() {
        return this.lastLocalTimestamp;
    }

    public void setLastLocalTimestamp(long lastLocalTimestamp) {
        synchronized (lock) {
            this.lastLocalTimestamp = lastLocalTimestamp;
        }
    }

    public Node getSuccessor() {
        return successor;
    }

    public void setSuccessor(Node successor) {
        synchronized (lock) {
            this.successor = successor;
        }
    }

    public Node getPredecessor() {
        return predecessor;
    }

    public void setPredecessor(Node predecessor) {
        synchronized (lock) {
            this.predecessor = predecessor;
        }
    }

    public BigInteger getLowerBound() {
        return this.lowerBound;
    }

    public BigInteger getUpperBound() {
        return this.upperBound;
    }

    public List<Node> getReplicas() {
        // return replicas;
        List<Node> replicas = new ArrayList<>();
        replicas.add(this.successor);
        replicas.add(this.successor.getSuccessor());
        replicas.add(this.successor.getSuccessor().getSuccessor());
        return replicas;
    }

    // public void addReplica(Node replica) {
    // synchronized (lock) {
    // this.replicas.add(replica);
    // }
    // }

    // public boolean removeReplica(Node replica) {
    // synchronized (lock) {
    // return this.replicas.remove(replica);
    // }
    // }

    // public void clearReplicas() {
    // synchronized (lock) {
    // this.replicas.clear();
    // }
    // }

    public Node getReplica(int nodeId) {
        // for (Node replica : replicas) {
        // if (replica.getNodeId() == nodeId) {
        // return replica;
        // }
        // }
        List<Node> replicas = getReplicas();
        for (Node replica : replicas) {
            if (replica.getNodeId() == nodeId) {
                return replica;
            }
        }
        return null;
    }

    public Object getExternalLock() {
        return externalLock;
    }
}
