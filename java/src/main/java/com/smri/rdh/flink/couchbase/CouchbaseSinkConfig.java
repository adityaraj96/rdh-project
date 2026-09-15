package com.smri.rdh.flink.couchbase;

import java.io.Serializable;
import java.util.Objects;

public final class CouchbaseSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    final String connectionString;   
    final String username;
    final String password;           
    final String bucket;
    final String scope;
    final String collection;

    final int batchSize;             
    final long lingerMs;             
    final int maxConcurrency;        
    final long kvTimeoutMs;          
    final long flushTimeoutMs;       
    final int maxRetries;            

    CouchbaseSinkConfig(Builder b) {
        this.connectionString = Objects.requireNonNull(b.connectionString, "connectionString");
        this.username = Objects.requireNonNull(b.username, "username");
        this.password = Objects.requireNonNull(b.password, "password");
        this.bucket = Objects.requireNonNull(b.bucket, "bucket");
        this.scope = b.scope == null ? "_default" : b.scope;
        this.collection = b.collection == null ? "_default" : b.collection;
        this.batchSize = b.batchSize;
        this.lingerMs = b.lingerMs;
        this.maxConcurrency = b.maxConcurrency;
        this.kvTimeoutMs = b.kvTimeoutMs;
        this.flushTimeoutMs = b.flushTimeoutMs;
        this.maxRetries = b.maxRetries;
    }

    String clusterKey() {
        return connectionString + "|" + username;
    }

    public static final class Builder {
        private String connectionString, username, password, bucket, scope, collection;
        private int batchSize = 500;
        private long lingerMs = 200;
        private int maxConcurrency = 128;
        private long kvTimeoutMs = 2_500;
        private long flushTimeoutMs = 15_000; // PERFECTION FIX: Reduced from 60_000
        private int maxRetries = 10;

        public Builder connectionString(String v) { this.connectionString = v; return this; }
        public Builder username(String v) { this.username = v; return this; }
        public Builder password(String v) { this.password = v; return this; }
        public Builder bucket(String v) { this.bucket = v; return this; }
        public Builder scope(String v) { this.scope = v; return this; }
        public Builder collection(String v) { this.collection = v; return this; }
        public Builder batchSize(int v) { this.batchSize = v; return this; }
        public Builder lingerMs(long v) { this.lingerMs = v; return this; }
        public Builder maxConcurrency(int v) { this.maxConcurrency = v; return this; }
        public Builder kvTimeoutMs(long v) { this.kvTimeoutMs = v; return this; }
        public Builder flushTimeoutMs(long v) { this.flushTimeoutMs = v; return this; }
        public Builder maxRetries(int v) { this.maxRetries = v; return this; }

        public CouchbaseGuardedSink build() {
            return new CouchbaseGuardedSink(new CouchbaseSinkConfig(this));
        }
    }
}