package com.g10.CPEN431.A11;

import ca.NetSysLab.ProtocolBuffers.*;
import com.g10.CPEN431.A11.KVStore.Value;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class Server {
    static final int MAX_KEY_SIZE = 32, MAX_VALUE_SIZE = 10000;
    static final int MEM_PAD = 2 * 1024 * 1024;
    static final int CACHE_SIZE = 20 * 1024 * 1024;
    static final int QUEUE_SIZE = 100;

    static final int E_SUCCESS = 0x00, E_NOKEY = 0x01, E_SPACE = 0x02, E_OVERLOAD = 0x03, E_INTERNAL = 0x04,
            E_COMMAND = 0x05, E_KEY = 0x06, E_VAL = 0x07, E_ACK = 0x21;
    static final int PUT = 0x01, GET = 0x02, REMOVE = 0x03, SHUTDOWN = 0x04, WIPEOUT = 0x05, ISALIVE = 0x06,
            GETPID = 0x07, GETMEMBERCOUNT = 0x08, PUT_TRANSFER = 0x22;

    static final int COMMAND_RESULT_SIZE = 44;
    static final int SYSTEM_OVERLOAD_WAIT_TIME = 250;

    static final long MAX_TIME_DIFF_IN_SECONDS = 2;
    static final double EPIDEMIC_STOP_PROPAGATION_PROB = 1.0;

    public static final int NO_TRANSFER_PERIOD = 15000;

    public int port;
    public InetAddress ip;
    public DatagramSocket socket;
    public DatagramSocket dataTransferSocket;
    public Router router;
    public Cache<ByteString, Pair<KeyValueResponse.KVResponse, Integer>> cache;
    public Cache<ByteString, KeyValueResponse.KVResponse> ackCache;
    public Map<Integer, Pair<byte[], Long>> internalCache;
    public Map<String, Pair<byte[], Integer>> keyValMap;
    public KVStore kvStore;
    public int systemOverloadCounterKVStore;
    public long lastOverloadResponseKVStore;
    public int serverNodeId;
    public int pid;

    private final Object cacheLock = new Object();
    private final Object ackCacheLock = new Object();
    private final Object nodeDataTransferLock = new Object();
    private final Object socketLock = new Object();
    private final Object systemOverloadLock = new Object();
    private final Object setIsAliveNodeLock = new Object();
    private static int NUM_NODES_CHOSEN_FOR_EPIDEMIC;
    private static int NODE_EXPIRATION_TIME; // seconds

    public static Object isAliveLock = new Object();
    public static long isAliveTimestamp = System.currentTimeMillis();
    public static long isAliveRunningTimestamp = System.currentTimeMillis();

    public Server(InetAddress ip, int port, Router router, int numNodesChosenForEpidemic, int nodeExpirationTime)
            throws SocketException {
        this.keyValMap = new ConcurrentHashMap<>();
        this.cache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.SECONDS).build();
        this.ackCache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.SECONDS).build();
        this.ip = ip;
        this.port = port;
        this.socket = new DatagramSocket(port);
        this.dataTransferSocket = new DatagramSocket(port + 1);
        this.router = router;
        this.systemOverloadCounterKVStore = 0;
        this.lastOverloadResponseKVStore = 0;
        this.serverNodeId = this.router.getNodeByAddress(this.ip, this.port).getNodeId();
        this.pid = (int) ProcessHandle.current().pid();
        NUM_NODES_CHOSEN_FOR_EPIDEMIC = numNodesChosenForEpidemic;
        NODE_EXPIRATION_TIME = nodeExpirationTime;

        this.kvStore = new KVStore(this.router.getNumNodes(), this.router);
    }

    Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
        public void uncaughtException(Thread th, Throwable ex) {
            ex.printStackTrace();
            System.out.println("Uncaught exception in thread '" + th.getName() + "': " + ex);
        }
    };

    ThreadFactory threadFactory = new ThreadFactory() {
        public Thread newThread(Runnable r) {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setUncaughtExceptionHandler(handler);
            return t;
        }
    };

    public void listen() {
        ExecutorService executorService = new ThreadPoolExecutor(20, 25, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), threadFactory);

        ExecutorService replicaService = new ThreadPoolExecutor(5, 5, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), threadFactory);
        ExecutorService nodeJoinService = new ThreadPoolExecutor(3, 5, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), threadFactory);
        ScheduledExecutorService epidemicService = Executors.newScheduledThreadPool(1);

        // Schedule epidemic thread with a fixed delay
        epidemicService.scheduleWithFixedDelay(
                new Epidemic(router, serverNodeId, socket, socketLock, kvStore, NUM_NODES_CHOSEN_FOR_EPIDEMIC,
                        NODE_EXPIRATION_TIME, setIsAliveNodeLock),
                0, 800, TimeUnit.MILLISECONDS);

        ServerThread mainRunnable = new ServerThread(socket, executorService, replicaService, nodeJoinService);
        ServerThread dataTransferRunnable = new ServerThread(dataTransferSocket, executorService, replicaService,
                nodeJoinService);

        Thread mainServerThread = new Thread(mainRunnable);
        Thread dataTransferServerThread = new Thread(dataTransferRunnable);

        mainServerThread.start();
        dataTransferServerThread.start();

        try {
            mainServerThread.join();
            dataTransferServerThread.join();
        } catch (InterruptedException e) {
            System.out.println("Exception in joining threads");
            e.printStackTrace();
        }
    }

    class ServerThread implements Runnable {
        ExecutorService executorService;
        ExecutorService replicaService;
        ExecutorService nodeJoinService;
        DatagramSocket socket;

        public ServerThread(DatagramSocket socket, ExecutorService executorService, ExecutorService replicaService,
                ExecutorService nodeJoinService) {
            this.socket = socket;
            this.executorService = executorService;
            this.replicaService = replicaService;
            this.nodeJoinService = nodeJoinService;
        }

        @Override
        public void run() {
            byte[] buf = new byte[Utils.BUFFER_SIZE];

            while (true) {
                DatagramPacket packet = new DatagramPacket(buf, buf.length);

                try {
                    this.socket.receive(packet);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                Message.Msg reqMsg = null;
                try {
                    reqMsg = Message.Msg.parseFrom(Arrays.copyOf(packet.getData(), packet.getLength()));
                } catch (InvalidProtocolBufferException e) {
                    System.out.println("InvalidProtocolBufferException: " + e.getMessage());
                    throw new RuntimeException(e);
                }

                executorService.submit(new RequestHandler(replicaService, nodeJoinService,
                        packet, reqMsg));
            }
        }
    }

    class RequestHandler implements Runnable {
        private Message.Msg reqMsg;
        private DatagramPacket packet;
        private ExecutorService replicaService;
        private ExecutorService nodeJoinService;

        public RequestHandler(ExecutorService replicaService, ExecutorService nodeJoinService, DatagramPacket packet,
                Message.Msg reqMsg) {
            this.packet = packet;
            this.reqMsg = reqMsg;
            this.replicaService = replicaService;
            this.nodeJoinService = nodeJoinService;
        }

        @Override
        public void run() {
            updateIsAliveTimestamp(kvStore);

            try {
                KeyValueResponse.KVResponse kvResponse;
                Pair<KeyValueResponse.KVResponse, Integer> cacheResult;
                Message.Msg reqMsg = this.reqMsg;

                ByteString messageID = reqMsg.getMessageID();
                boolean areCheckSumsEqual;

                if (reqMsg.getType() == Utils.FORWARDED_MSG || reqMsg.getType() == Utils.EPIDEMIC_MSG
                        || reqMsg.getType() == Utils.KV_TRANSFER_MSG) {
                    areCheckSumsEqual = reqMsg.getCheckSum() == SerializeUtils.calcChecksum(messageID.toByteArray(),
                            reqMsg.getPayload().toByteArray(), reqMsg.getType());
                } else {
                    areCheckSumsEqual = reqMsg.getCheckSum() == SerializeUtils.calcChecksum(messageID.toByteArray(),
                            reqMsg.getPayload().toByteArray());
                }

                KeyValueRequest.KVRequest kvRequest;
                InetAddress clientAddress;
                int clientPort;
                byte[] clientPayload;

                if (!areCheckSumsEqual) {
                    System.out.println("Checksums don't match");
                    return;
                }

                switch (reqMsg.getType()) {
                    case Utils.FORWARDED_MSG: {
                        byte[] payload = reqMsg.getPayload().toByteArray();
                        ForwardedMessage.FwdMsg fwdMsg;
                        try {
                            fwdMsg = ForwardedMessage.FwdMsg.parseFrom(payload);
                        } catch (InvalidProtocolBufferException e) {
                            return;
                        }
                        messageID = fwdMsg.getClientMessageId();
                        clientAddress = InetAddress.getByName(fwdMsg.getClientIP());
                        clientPort = fwdMsg.getClientPort();
                        clientPayload = fwdMsg.getClientRequest().toByteArray();
                        break;
                    }

                    case Utils.EPIDEMIC_MSG: {
                        byte[] payloadBytes = reqMsg.getPayload().toByteArray();
                        EpidemicPayload.EpidemicPld payload = EpidemicPayload.EpidemicPld.parseFrom(payloadBytes);
                        int payloadNodeId = payload.getNodeID();

                        if (serverNodeId == payloadNodeId) {
                            return;
                        }

                        Node payloadNode = router.getNodeById(payloadNodeId);
                        if (payloadNode == null) {
                            // invalid node id
                            return;
                        }

                        if (payload.getTimestamp() < payloadNode.getLastIsAliveMessageTimestamp()) {
                            // this message is old, don't process it
                            return;
                        }

                        double propagationProbability = Math.random();
                        boolean propagateMessage = propagationProbability <= payload.getPropagationProbability();

                        // make sure the epidemic message is new (not already received)
                        synchronized (setIsAliveNodeLock) {
                            if (payload.getTimestamp() != payloadNode.getLastIsAliveMessageTimestamp()) {
                                if (!payloadNode.getIsAlive()) {
                                    System.out.println("Node " + payloadNodeId + " joined!");
                                    payloadNode.setIsAlive(true);
                                    router.insertRoute(payloadNodeId);

                                    Node serverNode = router.getNodeById(serverNodeId);

                                    long currentTime = System.currentTimeMillis();
                                    if (currentTime - isAliveTimestamp >= NO_TRANSFER_PERIOD) {

                                        boolean isNodeDataTransferRequired = TransferNodeData
                                                .isNodeDataTransferRequired(
                                                        kvStore,
                                                        router,
                                                        serverNode,
                                                        payloadNode);

                                        if (isNodeDataTransferRequired) {
                                            TransferNodeData transferNodeData = new TransferNodeData(kvStore, router,
                                                    payloadNode, nodeDataTransferLock);

                                            nodeJoinService.submit(transferNodeData);
                                        }

                                        boolean isReplicaTransferRequired = TransferReplicaData
                                                .isReplicaTransferRequired(kvStore, router, serverNode, payloadNode);
                                        // System.out.println("Is replica transfer required: " +
                                        // isReplicaTransferRequired);

                                        if (isReplicaTransferRequired) {
                                            Node joiningReplicaNode = null;
                                            for (Node replicaNode : serverNode.getReplicas()) {
                                                if (replicaNode.getNodeId() == payloadNode.getNodeId()) {
                                                    joiningReplicaNode = replicaNode;
                                                    break;
                                                }
                                            }

                                            if (joiningReplicaNode == null) {
                                                System.out.println(
                                                        "Replica node not owned for node " + payloadNode.getNodeId()
                                                                + " , no need to transfer data");
                                            } else {
                                                TransferReplicaData transferReplicaData = new TransferReplicaData(
                                                        kvStore, router, joiningReplicaNode);

                                                nodeJoinService.submit(transferReplicaData);
                                            }
                                        }
                                    } else {
                                        // System.out.println("Node data transfer is waiting for " + NO_TRANSFER_PERIOD
                                        // + " ms to complete");
                                    }
                                }

                                payloadNode.setLastIsAliveMessageId(messageID);
                                payloadNode.setLastIsAliveMessageTimestamp(payload.getTimestamp());
                                payloadNode.setLastLocalTimestamp(System.nanoTime());
                            }
                        }

                        if (propagateMessage) {
                            Collection<Node> nodes = router
                                    .getRandomNodes(NUM_NODES_CHOSEN_FOR_EPIDEMIC, serverNodeId, payloadNodeId)
                                    .values();
                            for (Node node : nodes) {
                                // get the epidemic message from the req message
                                EpidemicPayload.EpidemicPld ePayload = EpidemicPayload.EpidemicPld
                                        .newBuilder()
                                        .setNodeID(payloadNodeId)
                                        .setTimestamp(payload.getTimestamp())
                                        .setPropagationProbability(payload.getPropagationProbability() / 2)
                                        .build();

                                byte[] fwdMsg = SerializeUtils.serializeMessage(messageID.toByteArray(),
                                        ePayload.toByteArray(), Utils.EPIDEMIC_MSG);

                                DatagramPacket packet = new DatagramPacket(fwdMsg, fwdMsg.length, node.getIP(),
                                        node.getPort());
                                synchronized (socketLock) {
                                    socket.send(packet);
                                }
                            }
                        }
                        return;
                    }

                    case Utils.KV_TRANSFER_MSG: {
                        // System.out.println("Received KV TRANSFER MSG");
                        synchronized (ackCacheLock) {
                            KeyValueResponse.KVResponse cachedResponse;
                            cachedResponse = ackCache.getIfPresent(messageID);
                            if (cachedResponse != null) {
                                // System.out.println("ACK already sent for message ID " +
                                // Arrays.toString(messageID.toByteArray()));

                                // synchronized (dataTransferSocketLock) {
                                // dataTransferSocket.send(new DatagramPacket(cachedResponse.toByteArray(),
                                // cachedResponse.toByteArray().length, packet.getAddress(),
                                // packet.getPort()));
                                // }
                                return;
                            }

                            byte[] payloadBytes = reqMsg.getPayload().toByteArray();
                            KeyValueTransfer.KVTransfer kvTransfer = KeyValueTransfer.KVTransfer
                                    .parseFrom(payloadBytes);

                            if (kvTransfer.getCommand() == WIPEOUT) {
                                kvStore.wipeout(kvTransfer.getWipeoutBucketId());
                            } else {
                                kvStore.putTransfer(kvTransfer.getKey(), kvTransfer.getValue().toByteArray(),
                                        kvTransfer.getVersion(), kvTransfer.getLpClock());
                            }

                            // System.out.println("Packet received from " +
                            // packet.getAddress().getHostAddress() + ":"
                            // + packet.getPort());

                            // KeyValueResponse.KVResponse ackResponse = sendAck(packet, messageID);
                            KeyValueResponse.KVResponse ackResponse = KeyValueResponse.KVResponse.newBuilder()
                                    .setErrCode(E_ACK).build();
                            ackCache.put(messageID, ackResponse);
                        }
                        return;
                    }

                    default: {
                        clientAddress = packet.getAddress();
                        clientPort = packet.getPort();
                        clientPayload = reqMsg.getPayload().toByteArray();
                        break;
                    }
                }

                kvRequest = KeyValueRequest.KVRequest.parseFrom(clientPayload);

                if (kvRequest.getKey().toByteArray().length > MAX_KEY_SIZE) {
                    kvResponse = KeyValueResponse.KVResponse.newBuilder().setErrCode(E_KEY).build();
                } else if (kvRequest.getValue().toByteArray().length > MAX_VALUE_SIZE) {
                    kvResponse = KeyValueResponse.KVResponse.newBuilder().setErrCode(E_VAL).build();
                } else {
                    synchronized (cacheLock) {
                        cacheResult = cache.getIfPresent(messageID);
                        if (cacheResult != null) {
                            kvResponse = cacheResult.getFirstVal();
                        } else {
                            Integer nodeId;
                            nodeId = router.getNodeIdByKey(router.hashKey(kvRequest.getKey().toByteArray()));

                            if (nodeId == null) {
                                System.out.println("No node found. Oops!");
                                return;
                            } else {
                                Node node = router.getNodeById(nodeId);

                                if (nodeId != serverNodeId && (kvRequest.getCommand() == PUT
                                        || kvRequest.getCommand() == GET || kvRequest.getCommand() == REMOVE)) {

                                    ForwardedMessage.FwdMsg fwdMsg = ForwardedMessage.FwdMsg.newBuilder()
                                            .setClientMessageId(messageID).setClientIP(clientAddress.getHostAddress())
                                            .setClientPort(clientPort).setClientRequest(kvRequest.toByteString())
                                            .build();
                                    byte[] fwdReqMessageId = SerializeUtils.generateMessageID();
                                    byte[] fwdReqMsg = SerializeUtils.serializeMessage(fwdReqMessageId,
                                            fwdMsg.toByteArray(), Utils.FORWARDED_MSG);

                                    packet = new DatagramPacket(fwdReqMsg, fwdReqMsg.length, node.getIP(),
                                            node.getPort());
                                    synchronized (socketLock) {
                                        socket.send(packet);
                                    }
                                    return;
                                } else {
                                    kvResponse = handleCommand(kvRequest.getCommand(), kvRequest.getKey(),
                                            kvRequest.getValue(), kvRequest.getVersion());

                                    cache.put(messageID, new Pair<>(kvResponse, 1));
                                }
                            }
                        }
                    }
                }

                if (kvResponse.getErrCode() == E_SUCCESS && (kvRequest.getCommand() == PUT
                        || kvRequest.getCommand() == REMOVE)) {
                    sendRequestToReplicas(kvRequest);
                }

                byte[] resPayload = kvResponse.toByteArray();
                byte[] response = SerializeUtils.serializeMessage(messageID.toByteArray(), resPayload);

                packet = new DatagramPacket(response, response.length, clientAddress, clientPort);
                synchronized (socketLock) {
                    socket.send(packet);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private KeyValueResponse.KVResponse handleCommand(int command, ByteString key, ByteString val, int version) {
            KeyValueResponse.KVResponse kvResponse;
            byte[] value = val.toByteArray();

            switch (command) {
                case PUT: {
                    // long freeMemory = Runtime.getRuntime().freeMemory();
                    long freeMemory = Runtime.getRuntime().maxMemory()
                            - (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());

                    if (freeMemory < MEM_PAD) {
                        synchronized (systemOverloadLock) {
                            if (systemOverloadCounterKVStore < 10) {
                                // System.out.println("System Overload");
                                kvResponse = KeyValueResponse.KVResponse.newBuilder().setErrCode(E_OVERLOAD)
                                        .setOverloadWaitTime(SYSTEM_OVERLOAD_WAIT_TIME).build();

                                if (System.currentTimeMillis()
                                        - lastOverloadResponseKVStore >= SYSTEM_OVERLOAD_WAIT_TIME) {
                                    cache.cleanUp();
                                    System.gc();
                                    lastOverloadResponseKVStore = System.currentTimeMillis();
                                    systemOverloadCounterKVStore++;
                                }
                            } else {
                                // System.out.println("System Out of Space");
                                kvResponse = KeyValueResponse.KVResponse.newBuilder().setErrCode(E_SPACE).build();
                            }
                            return kvResponse;
                        }
                    }

                    kvStore.put(key, value, version);
                    kvResponse = KeyValueResponse.KVResponse.newBuilder().setErrCode(E_SUCCESS).build();
                    return kvResponse;
                }

                case GET: {
                    Value rVal = kvStore.get(key);
                    if (rVal == null) {
                        kvResponse = KeyValueResponse.KVResponse.newBuilder().setErrCode(E_NOKEY).build();
                        // System.out.println("GET: Key not found");
                    } else {
                        kvResponse = KeyValueResponse.KVResponse.newBuilder().setErrCode(E_SUCCESS)
                                .setValue(ByteString.copyFrom(rVal.getValue()))
                                .setVersion(rVal.getVersion()).build();
                    }
                    return kvResponse;
                }

                case REMOVE: {
                    System.out.println("REMOVE command");
                    Value rVal = kvStore.remove(key);
                    if (rVal == null) {
                        kvResponse = KeyValueResponse.KVResponse.newBuilder().setErrCode(E_NOKEY).build();
                    } else {
                        kvResponse = KeyValueResponse.KVResponse.newBuilder().setErrCode(E_SUCCESS).build();
                    }
                    return kvResponse;
                }

                case SHUTDOWN: {
                    System.exit(0);
                }

                case WIPEOUT: {
                    // System.out.println("WIPEOUT command");
                    kvStore.wipeout();
                    cache.invalidateAll();
                    System.gc();
                    kvResponse = KeyValueResponse.KVResponse.newBuilder().setErrCode(E_SUCCESS).build();

                    synchronized (systemOverloadLock) {
                        systemOverloadCounterKVStore = 0;
                    }

                    return kvResponse;
                }

                case ISALIVE: {
                    kvResponse = KeyValueResponse.KVResponse.newBuilder().setErrCode(E_SUCCESS).build();
                    return kvResponse;
                }

                case GETPID: {
                    kvResponse = KeyValueResponse.KVResponse.newBuilder().setErrCode(E_SUCCESS).setPid(pid).build();
                    return kvResponse;
                }

                case GETMEMBERCOUNT: {
                    kvResponse = KeyValueResponse.KVResponse.newBuilder().setErrCode(E_SUCCESS)
                            .setMembershipCount(router.getMembershipCount()).build();
                    return kvResponse;
                }

                default: {
                    kvResponse = KeyValueResponse.KVResponse.newBuilder().setErrCode(E_COMMAND).build();
                    return kvResponse;
                }
            }
        }

        private void sendRequestToReplicas(KeyValueRequest.KVRequest kvRequest) throws SocketException {
            KeyValueTransfer.KVTransfer kvTransfer;

            switch (kvRequest.getCommand()) {
                case WIPEOUT: {
                    kvTransfer = KeyValueTransfer.KVTransfer.newBuilder()
                            .setCommand(kvRequest.getCommand())
                            .setWipeoutBucketId(serverNodeId)
                            .build();
                    break;
                }

                case REMOVE: {
                    kvTransfer = KeyValueTransfer.KVTransfer.newBuilder()
                            .setCommand(kvRequest.getCommand())
                            .setKey(kvRequest.getKey())
                            .build();
                    break;
                }

                case PUT: {
                    Value val = kvStore.get(kvRequest.getKey());
                    if (val == null) {
                        System.out.println("sendRequestToReplicas: Null value");
                        return;
                    }
                    kvTransfer = KeyValueTransfer.KVTransfer.newBuilder()
                            .setCommand(kvRequest.getCommand())
                            .setKey(kvRequest.getKey())
                            .setValue(ByteString.copyFrom(val.getValue()))
                            .setVersion(val.getVersion())
                            .setLpClock(val.getLpClock())
                            .build();

                    // kvStore.getBucketId(kvRequest.getKey());

                    break;
                }

                default: {
                    System.out.println("Invalid command. UNEXPECTED REQUEST");
                    return;
                }
            }

            byte[] msgID = SerializeUtils.generateMessageID();

            byte[] kvTransferMsg = SerializeUtils.serializeMessage(msgID, kvTransfer.toByteArray(),
                    Utils.KV_TRANSFER_MSG);

            Node serverNode = router.getNodeById(serverNodeId);

            for (Node replicaNode : serverNode.getReplicas()) {
                PrimaryReplica primaryReplica = new PrimaryReplica(replicaNode, kvTransferMsg, socket, socketLock);
                replicaService.submit(primaryReplica);
            }
        }
    }

    public static void updateIsAliveTimestamp(KVStore kvStore) {
        synchronized (isAliveLock) {
            long currentTime = System.currentTimeMillis();
            if (currentTime - isAliveRunningTimestamp >= MAX_TIME_DIFF_IN_SECONDS * 1000) {
                System.out.println("Node identifies itself as a joining node!");
                isAliveTimestamp = currentTime;
                kvStore.wipeout();
            }
            isAliveRunningTimestamp = currentTime;
        }
    }

    // private KeyValueResponse.KVResponse sendAck(DatagramPacket packet, ByteString
    // messageId) throws IOException {
    // KeyValueResponse.KVResponse ackResponse =
    // KeyValueResponse.KVResponse.newBuilder().setErrCode(E_ACK)
    // .build();

    // byte[] ackMsg = SerializeUtils.serializeMessage(messageId.toByteArray(),
    // ackResponse.toByteArray(),
    // Utils.KV_TRANSFER_MSG);

    // DatagramPacket ackPacket = new DatagramPacket(ackMsg, ackMsg.length,
    // packet.getAddress(),
    // packet.getPort());
    // synchronized (dataTransferSocketLock) {
    // dataTransferSocket.send(ackPacket);
    // }

    // return ackResponse;
    // }
}
