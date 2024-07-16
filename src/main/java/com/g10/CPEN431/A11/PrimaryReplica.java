package com.g10.CPEN431.A11;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

public class PrimaryReplica implements Runnable {
    private Node replicaNode;
    private byte[] reqMsg;
    private DatagramSocket socket;
    private Object socketLock;

    public PrimaryReplica(Node replicaNode, byte[] reqMsg, DatagramSocket socket, Object socketLock)
            throws SocketException {
        this.replicaNode = replicaNode;
        this.reqMsg = reqMsg;
        this.socket = socket;
        this.socketLock = socketLock;
    }

    @Override
    public void run() {
        synchronized (replicaNode.getExternalLock()) {
            // check if the replica is alive
            if (!replicaNode.getIsAlive()) {
                return;
            }

            DatagramPacket packet = new DatagramPacket(reqMsg, reqMsg.length, replicaNode.getIP(),
                    replicaNode.getPort() + 1);
            try {
                synchronized (socketLock) {
                    this.socket.send(packet);
                    this.socket.send(packet);
                }
                return;

            } catch (IOException e) {
                System.out.println("ReplicaNode Socket IO Exception!");
            }            
        }
    }
}
