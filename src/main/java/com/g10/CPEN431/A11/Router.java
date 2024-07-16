package com.g10.CPEN431.A11;

import java.math.BigInteger;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Router {
    private Map<Integer, Node> routeMap;
    private Object lock;
    static BigInteger maxInteger = new BigInteger("2").pow(256);

    public Router(List<Pair<InetAddress, Integer>> addresses) {
        this.lock = new Object();

        int numAddresses = addresses.size();
        maxInteger = maxInteger.subtract(new BigInteger("1"));
        BigInteger range = maxInteger.divide(new BigInteger(Integer.toString(numAddresses)));
        this.routeMap = new ConcurrentHashMap<>();

        for (int i = 0; i < numAddresses; i++) {
            Pair<InetAddress, Integer> address = addresses.get(i);
            InetAddress ip = address.getFirstVal();
            int port = address.getSecondVal();

            BigInteger minKey = range.multiply(new BigInteger(Integer.toString(i)));
            BigInteger maxKey = range.multiply(new BigInteger(Integer.toString(i + 1)));
            if (i != numAddresses - 1) {
                maxKey = maxKey.subtract(new BigInteger("1"));
            } else {
                maxKey = maxInteger;
            }

            addRoute(i, ip, port, minKey, maxKey);
        }

        // set the successor and predecessor
        int numNodes = getNumNodes();
        for (int nodeId = 0; nodeId < numNodes; nodeId++) {
            setSuccessorAndPredecessor(nodeId);
        }

        // set replicas
        // for (int nodeId = 0; nodeId < numNodes; nodeId++) {
        //     setReplicas(nodeId);
        // }
    }

    private void addRoute(int nodeId, InetAddress ip, int port, BigInteger minKey, BigInteger maxKey) {
        routeMap.put(nodeId, new Node(nodeId, ip, port, true, minKey, maxKey));
    }

    private void setSuccessorAndPredecessor(int nodeId) {
        int numNodes = getNumNodes();
        Node node = routeMap.get(nodeId);
        int successorId = (nodeId + 1) % numNodes;
        int predecessorId = (nodeId - 1 + numNodes) % numNodes;
        node.setSuccessor(routeMap.get(successorId));
        node.setPredecessor(routeMap.get(predecessorId));
    }

    // private void setReplicas(int nodeId) {
    //     Node parentNode = routeMap.get(nodeId);
    //     parentNode.addReplica(parentNode.getSuccessor());
    //     parentNode.addReplica(parentNode.getSuccessor().getSuccessor());
    //     parentNode.addReplica(parentNode.getSuccessor().getSuccessor().getSuccessor());
    // }

    public Node getNodeByAddress(InetAddress ip, int port) {
        String address = ip.getHostAddress() + ":" + port;
        for (Map.Entry<Integer, Node> entry : routeMap.entrySet()) {
            Node node = entry.getValue();
            if (node.getAddress().equals(address)) {
                return node;
            }
        }
        return null;
    }

    public Node getNodeById(int nodeId) {
        return routeMap.get(nodeId);
    }

    public void invalidateRoute(int nodeId) {
        synchronized (lock) {
            Node deadNode = routeMap.get(nodeId);
            deadNode.setIsAlive(false);
            Node successorNode = deadNode.getSuccessor();
            Node predecessorNode = deadNode.getPredecessor();

            predecessorNode.setSuccessor(successorNode);
            successorNode.setPredecessor(predecessorNode);
            successorNode.setMinKey(deadNode.getMinKey());
        }
    }

    public void insertRoute(int nodeId) {
        synchronized (lock) {
            int numNodes = getNumNodes();
            Node newNode = routeMap.get(nodeId);

            // update successor
            int successorId = (nodeId + 1) % numNodes;

            Node successor = routeMap.get(successorId);
            while (!successor.getIsAlive()) {
                successor = routeMap.get((++successorId) % numNodes);
            }

            Node predecessor = successor.getPredecessor();
            newNode.setSuccessor(successor);
            newNode.setPredecessor(predecessor);
            predecessor.setSuccessor(newNode);
            successor.setPredecessor(newNode);
            newNode.setMinKey(successor.getMinKey());

            int newNodesNextNodeId = (nodeId + 1) % numNodes;
            Node newNodesNextNode = routeMap.get(newNodesNextNodeId);
            successor.setMinKey(newNodesNextNode.getLowerBound());
        }
    }

    public Integer getNodeIdByKey(BigInteger key) {
        BigInteger maxKey = maxInteger.subtract(new BigInteger("1"));
        BigInteger minKey = new BigInteger("0");

        for (Map.Entry<Integer, Node> entry : routeMap.entrySet()) {
            Node node = entry.getValue();
            if (node.getIsAlive()) {
                if (node.getMinKey().compareTo(node.getMaxKey()) > 0) {
                    // minKey is bigger than maxKey
                    // if (key.compareTo(node.getMinKey()) >= 0 || key.compareTo(node.getMaxKey())
                    // <= 0){
                    // return entry.getKey();
                    // }

                    if ((key.compareTo(node.getMinKey()) >= 0 && key.compareTo(maxKey) <= 0) ||
                            (key.compareTo(minKey) >= 0 && key.compareTo(node.getMaxKey()) <= 0)) {
                        return entry.getKey();
                    }
                } else {
                    // maxKey is bigger than minKey
                    if (key.compareTo(node.getMinKey()) >= 0 && key.compareTo(node.getMaxKey()) <= 0) {
                        return entry.getKey();
                    }
                }
            }
        }
        return null;
    }

    // TODO: use binary search to improve performance
    public Integer getRawNodeIdByKey(BigInteger key) {
        for (Map.Entry<Integer, Node> entry : routeMap.entrySet()) {
            Node node = entry.getValue();
            if (key.compareTo(node.getLowerBound()) >= 0 && key.compareTo(node.getUpperBound()) <= 0) {
                return entry.getKey();
            }
        }
        return null;
    }

    // Sourced from ChatGPT
    public BigInteger hashKey(byte[] key) {
        try {
            // Create a MessageDigest instance for SHA-256
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            // Compute the hash value
            byte[] hashBytes = digest.digest(key);
            // Convert the hash bytes to a BigInteger
            return new BigInteger(1, hashBytes);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 is not available", e);
        }
    }

    public int getMembershipCount() {
        int count = 0;
        for (Map.Entry<Integer, Node> entry : routeMap.entrySet()) {
            if (entry.getValue().getIsAlive()) {
                count++;
            }
        }
        return count;
    }

    public Map<Integer, Node> getRandomNodes(int numNodes, int currNodeId, int originNodeId) {
        Map<Integer, Node> nodes = new HashMap<>();
        Random random = new Random();
        while (nodes.size() < numNodes) {
            int rand_int = random.nextInt(routeMap.size());
            if ((rand_int != currNodeId) && (rand_int != originNodeId) && (!nodes.containsKey(rand_int))) {
                nodes.put(rand_int, routeMap.get(rand_int));
            }
        }
        return nodes;
    }

    public int getNumNodes() {
        return routeMap.size();
    }

    public Collection<Node> getNodes() {
        return routeMap.values();
    }

    public List<Node> getOwnedNodes(Node node) {
        List<Node> ownedNodes = new ArrayList<>();
        int predecessorId = node.getPredecessor().getNodeId();
        while (predecessorId != node.getNodeId()) {
            ownedNodes.add(this.getNodeById((predecessorId + 1) % this.getNumNodes()));
            predecessorId = (predecessorId + 1) % this.getNumNodes();
        }

        return ownedNodes;
    }
}
