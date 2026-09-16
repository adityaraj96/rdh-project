package com.smri.rdh.flink.couchbase;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.types.Row;
import java.io.IOException;

public class CouchbaseGuardedSinkWriter implements SinkWriter<Row> {
    private final Cluster cluster;
    private final Collection collection;
    
    public CouchbaseGuardedSinkWriter(CouchbaseSinkConfig config) {
        this.cluster = ClusterRegistry.getCluster(config);
        this.collection = cluster.bucket(config.bucket).scope(config.scope).collection(config.collection);
    }

    @Override
    public void write(Row element, Context context) throws IOException {
        try {
            String op = (String) element.getField(0);
            String docKey = (String) element.getField(1);
            String payloadStr = (String) element.getField(3);

            if ("UPSERT".equalsIgnoreCase(op)) {
                collection.upsert(docKey, JsonObject.fromJson(payloadStr));
            } else if ("DELETE".equalsIgnoreCase(op)) {
                collection.remove(docKey);
            }
        } catch (Exception e) {
            throw new IOException("Failed to write to Couchbase", e);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        // Synchronous write handles state in this testing implementation
    }

    @Override
    public void close() throws Exception {
    }
}