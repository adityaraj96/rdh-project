package com.smri.rdh.flink.couchbase;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.types.Row;

public class CouchbaseGuardedSink implements Sink<Row> {
    private static final long serialVersionUID = 1L;

    private final CouchbaseSinkConfig config;

    CouchbaseGuardedSink(CouchbaseSinkConfig config) {
        this.config = config;
    }

    public static CouchbaseSinkConfig.Builder builder() {
        return new CouchbaseSinkConfig.Builder();
    }

    @Override
    public SinkWriter<Row> createWriter(WriterInitContext context) {
        return new CouchbaseGuardedSinkWriter(
                config, context.getProcessingTimeService(), context.metricGroup());
    }
}