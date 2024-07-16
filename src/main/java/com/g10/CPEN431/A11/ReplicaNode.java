package com.g10.CPEN431.A11;

import java.net.DatagramSocket;
import java.net.SocketException;

public class ReplicaNode {
    private Node node;
    private DatagramSocket socket;
    private Object externalLock;

    public ReplicaNode(Node node) throws SocketException {
        this.node = node;
        this.socket = new DatagramSocket();
        this.externalLock = new Object();
    }

    public Node getNode() {
        return node;
    }

    public DatagramSocket getSocket() {
        return socket;
    }

    public void setSocketTimeout(int timeout) throws SocketException {
        socket.setSoTimeout(timeout);
    }

    public Object getExternalLock() {
        return externalLock;
    }

}
