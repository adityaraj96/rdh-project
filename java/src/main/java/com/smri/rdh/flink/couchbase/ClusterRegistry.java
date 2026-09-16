package com.smri.rdh.flink.couchbase;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.env.ClusterEnvironment;
import java.time.Duration;

public class ClusterRegistry {
    private static volatile Cluster cluster;

    public static Cluster getCluster(CouchbaseSinkConfig config) {
        if (cluster == null) {
            synchronized (ClusterRegistry.class) {
                if (cluster == null) {
                    ClusterEnvironment env = ClusterEnvironment.builder()
                        .timeoutConfig(configurer -> configurer.kvTimeout(Duration.ofMillis(config.kvTimeoutMs)))
                        .build();
                    cluster = Cluster.connect(
                        config.connectionString,
                        ClusterOptions.clusterOptions(config.username, config.password).environment(env)
                    );
                }
            }
        }
        return cluster;
    }
}