package com.smri.rdh.flink.couchbase;

import java.io.Serializable;

public class CouchbaseSinkConfig implements Serializable {
    public final String connectionString;
    public final String username;
    public final String password;
    public final String bucket;
    public final String scope;
    public final String collection;
    public final int batchSize;
    public final int lingerMs;
    public final int maxConcurrency;
    public final int kvTimeoutMs;

    private CouchbaseSinkConfig(Builder builder) {
        this.connectionString = builder.connectionString;
        this.username = builder.username;
        this.password = builder.password;
        this.bucket = builder.bucket;
        this.scope = builder.scope;
        this.collection = builder.collection;
        this.batchSize = builder.batchSize;
        this.lingerMs = builder.lingerMs;
        this.maxConcurrency = builder.maxConcurrency;
        this.kvTimeoutMs = builder.kvTimeoutMs;
    }

    public static class Builder {
        private String connectionString;
        private String username;
        private String password;
        private String bucket = "default";
        private String scope = "_default";
        private String collection = "_default";
        private int batchSize = 500;
        private int lingerMs = 200;
        private int maxConcurrency = 128;
        private int kvTimeoutMs = 2500;

        public Builder connectionString(String connectionString) { this.connectionString = connectionString; return this; }
        public Builder username(String username) { this.username = username; return this; }
        public Builder password(String password) { this.password = password; return this; }
        public Builder bucket(String bucket) { this.bucket = bucket; return this; }
        public Builder scope(String scope) { this.scope = scope; return this; }
        public Builder collection(String collection) { this.collection = collection; return this; }
        public Builder batchSize(int batchSize) { this.batchSize = batchSize; return this; }
        public Builder lingerMs(int lingerMs) { this.lingerMs = lingerMs; return this; }
        public Builder maxConcurrency(int maxConcurrency) { this.maxConcurrency = maxConcurrency; return this; }
        public Builder kvTimeoutMs(int kvTimeoutMs) { this.kvTimeoutMs = kvTimeoutMs; return this; }
        
        public CouchbaseSinkConfig build() { return new CouchbaseSinkConfig(this); }
    }
}