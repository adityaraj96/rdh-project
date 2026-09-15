package com.smri.rdh.flink.couchbase;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

final class ClusterRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterRegistry.class);

    private static final class Holder {
        final Cluster cluster;
        int refs;
        Holder(Cluster c) { this.cluster = c; }
    }

    private static final Map<String, Holder> HOLDERS = new HashMap<>();

    private ClusterRegistry() {}

    static synchronized Cluster acquire(CouchbaseSinkConfig c) {
        Holder h = HOLDERS.get(c.clusterKey());
        if (h == null) {
            LOG.info("Connecting to Couchbase {}", c.connectionString);
            Cluster cluster = Cluster.connect(
                    c.connectionString,
                    ClusterOptions.clusterOptions(c.username, c.password)
                            .environment(env -> env
                                    .timeoutConfig(t -> t.kvTimeout(Duration.ofMillis(c.kvTimeoutMs)))));
            cluster.waitUntilReady(Duration.ofSeconds(30));
            h = new Holder(cluster);
            HOLDERS.put(c.clusterKey(), h);
        }
        h.refs++;
        return h.cluster;
    }

    static synchronized void release(CouchbaseSinkConfig c) {
        Holder h = HOLDERS.get(c.clusterKey());
        if (h != null && --h.refs <= 0) {
            HOLDERS.remove(c.clusterKey());
            LOG.info("Disconnecting from Couchbase {}", c.connectionString);
            h.cluster.disconnect();
        }
    }
}