package com.g10.CPEN431.A11;

import com.google.protobuf.ByteString;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class KVStore {
    public class Value {
        private byte[] value;
        private int version;
        private long lpClock;

        public Value(byte[] value, int version) {
            this.value = value;
            this.version = version;
            this.lpClock = 0;
        }

        public Value(byte[] value, int version, long lpClock) {
            this.value = value;
            this.version = version;
            this.lpClock = lpClock;
        }

        // override a copy function
        // public Value copy() {
        // return new Value(this.value, this.version, this.lpClock);
        // }

        public byte[] getValue() {
            return value;
        }

        public void setValue(byte[] value) {
            this.value = value;
        }

        public int getVersion() {
            return version;
        }

        public void setVersion(int version) {
            this.version = version;
        }

        public long getLpClock() {
            return lpClock;
        }

        public void setLpClock(long lpClock) {
            this.lpClock = lpClock;
        }
    }

    private List<ConcurrentHashMap<ByteString, Value>> listOfKeyValMaps;
    private Router router;

    public KVStore(int numBuckets, Router router) {
        listOfKeyValMaps = new ArrayList<>();
        for (int i = 0; i < numBuckets; i++) {
            this.listOfKeyValMaps.add(new ConcurrentHashMap<>());
        }

        this.router = router;
    }

    public void put(ByteString key, byte[] value, int version) {
        int bucketId = getBucketId(key);
        ConcurrentHashMap<ByteString, Value> keyValMap = listOfKeyValMaps.get(bucketId);

        // if (keyValMap.containsKey(key)) {
        // // System.out.println("PUT: Bucket ID " + bucketId + "contains the key: " +
        // // key.toString() + " , replacing...");

        // Value val = keyValMap.get(key);
        // val.setLpClock(val.getLpClock() + 1);
        // val.setValue(value);
        // val.setVersion(version);
        // } else {
        // // System.out.println("PUT: Bucket ID " + bucketId + " does not contain the
        // key:
        // // " + key.toString() + " , putting...");
        // keyValMap.put(key, new Value(value, version));
        // // System.out.println("PUT: Value put with version " + val.getVersion() + ",
        // // lpCLock " + val.getLpClock() + ", value")
        // }
        keyValMap.put(key, new Value(value, version));
    }

    public void putTransfer(ByteString key, byte[] value, int version, long lpClock) {
        int bucketId = getBucketId(key);
        ConcurrentHashMap<ByteString, Value> keyValMap = listOfKeyValMaps.get(bucketId);
        // if (keyValMap.containsKey(key)) {
        // // System.out.println(
        // // "PUT TRANSFER: Bucket ID " + bucketId + "contains the key: " +
        // key.toString()
        // // + " , replacing...");
        // Value val = keyValMap.get(key);
        // // if (val.getLpClock() > lpClock) {
        // // // System.out.println("curr value: " + Arrays.toString(val.getValue()));
        // // // System.out.println("PUT TRANSFER: Received value has lower lpClock than
        // // // current value, ignoring...");
        // // // System.out.println("Received lpClock: " + lpClock + ", Current lpClock:
        // "
        // // +
        // // // val.getLpClock());
        // // return;
        // // }
        // // System.out.println("PUT TRANSFER: Received value has higher lpClock than
        // // current value, updating...");
        // // System.out.println("Received lpClock: " + lpClock + ", Current lpClock: "
        // +
        // // val.getLpClock());

        // val.setLpClock(lpClock);
        // val.setValue(value);
        // val.setVersion(version);
        // // System.out.println("PUT TRANSFER: Received value has higher lpClock than
        // // current value, updating...");
        // // System.out.println("Received lpClock: " + lpClock + ", Current lpClock: "
        // +
        // // val.getLpClock());
        // } else {
        // // System.out.println("PUT TRANSFER: Bucket ID " + bucketId + " does not
        // contain
        // // the key: " + key.toString()
        // // + " , putting...");
        // keyValMap.put(key, new Value(value, version, lpClock));
        // // System.out.println("PUT TRANSFER: Received new key, putting...");
        // }
        keyValMap.put(key, new Value(value, version, lpClock));
    }

    public Value get(ByteString key) {
        int bucketId = getBucketId(key);
        ConcurrentHashMap<ByteString, Value> keyValMap = listOfKeyValMaps.get(bucketId);
        Value val = keyValMap.get(key);

        // if (keyValMap.containsKey(key)) {
        // // System.out.println("GET: Bucket ID " + bucketId + " contains the key: " +
        // // key.toString());
        // return keyValMap.get(key);
        // } else {
        // // System.out.println("GET: Bucket ID " + bucketId + " does not contain key:
        // "
        // // + Arrays.toString(key.toByteArray()));
        // }

        if (val == null) {
            System.out.println("KEY NOT FOUND");
        }
        return val;
    }

    public Value getInternal(ByteString key, int bucketId) {
        ConcurrentHashMap<ByteString, Value> keyValMap = listOfKeyValMaps.get(bucketId);

        Value val = keyValMap.get(key);
        if (val == null) {
            // System.out.println("GET INTERNAL: Key not found. Value is null.");
        }
        return val;
    }

    public Value remove(ByteString key) {
        int bucketId = getBucketId(key);
        ConcurrentHashMap<ByteString, Value> keyValMap = listOfKeyValMaps.get(bucketId);

        if (keyValMap.containsKey(key)) {
            return keyValMap.remove(key);
        }

        return null;
    }

    public void wipeout() {
        for (ConcurrentHashMap<ByteString, Value> keyValMap : listOfKeyValMaps) {
            keyValMap.clear();
        }
    }

    public void wipeout(int bucketId) {
        ConcurrentHashMap<ByteString, Value> keyValMap = listOfKeyValMaps.get(bucketId);
        keyValMap.clear();
    }

    public Set<ByteString> getKeys(int bucketId) {
        ConcurrentHashMap<ByteString, Value> keyValMap = listOfKeyValMaps.get(bucketId);
        return new HashSet<>(keyValMap.keySet());
    }

    public int getBucketId(ByteString key) {
        byte[] keyBytes = key.toByteArray();
        BigInteger hashedKey = router.hashKey(keyBytes);
        return router.getRawNodeIdByKey(hashedKey);
    }
}
