package com.smri.rdh.flink.couchbase;

import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.TemporaryFailureException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.kv.StoreSemantics;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

final class CouchbaseGuardedSinkWriter implements SinkWriter<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(CouchbaseGuardedSinkWriter.class);
    private static final Pattern SAFE_SECTION = Pattern.compile("[A-Za-z][A-Za-z0-9_]*");

    private final CouchbaseSinkConfig config;
    private final ProcessingTimeService timeService;
    private final Cluster cluster;
    private final ReactiveCollection collection;

    private final Map<String, Mutation> buffer = new LinkedHashMap<>();
    private boolean timerRegistered = false;

    private final Counter written, skippedStale, casRetries, invalid;

    CouchbaseGuardedSinkWriter(CouchbaseSinkConfig config,
                               ProcessingTimeService timeService,
                               SinkWriterMetricGroup metrics) {
        this.config = config;
        this.timeService = timeService;
        this.cluster = ClusterRegistry.acquire(config);
        this.collection = cluster.bucket(config.bucket)
                .scope(config.scope)
                .collection(config.collection)
                .reactive();

        var g = metrics.addGroup("couchbase");
        this.written = g.counter("written");
        this.skippedStale = g.counter("skippedStale");
        this.casRetries = g.counter("casRetries");
        this.invalid = g.counter("invalidRecords");
        g.gauge("bufferedMutations", (Gauge<Integer>) buffer::size);
    }

    @Override
    public void write(Row row, Context context) throws IOException {
        Mutation m = Mutation.fromRow(row);
        if (m == null) {
            invalid.inc();
            return; 
        }
        String k = m.docKey + '|' + m.section;
        Mutation existing = buffer.get(k);
        if (existing == null || m.ts >= existing.ts) {
            buffer.put(k, m); 
        }

        if (buffer.size() >= config.batchSize) {
            flushBuffer();
        } else if (!timerRegistered) {
            timerRegistered = true;
            long fireAt = timeService.getCurrentProcessingTime() + config.lingerMs;
            timeService.registerTimer(fireAt, t -> {
                timerRegistered = false;
                flushBuffer();
            });
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        flushBuffer(); 
    }

    @Override
    public void close() {
        ClusterRegistry.release(config);
    }

    private void flushBuffer() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }
        List<Mutation> batch = new ArrayList<>(buffer.values());
        buffer.clear();

        try {
            Flux.fromIterable(batch)
                    .groupBy(m -> m.docKey)
                    .flatMap(sameDoc -> sameDoc.concatMap(this::applyGuarded), config.maxConcurrency)
                    .then()
                    .block(Duration.ofMillis(config.flushTimeoutMs));
        } catch (RuntimeException e) {
            throw new IOException("Couchbase flush failed for batch of " + batch.size(), e);
        }
    }

    private Mono<Void> applyGuarded(Mutation m) {
        String tsPath = "_meta." + m.section + ".ts";

        Mono<Void> attempt = Mono.defer(() ->
                collection.lookupIn(m.docKey, Arrays.asList(
                                LookupInSpec.get(tsPath),        
                                LookupInSpec.exists(m.section))) 
                        .flatMap(current -> writeIfNewer(m, tsPath, current))
                        .onErrorResume(DocumentNotFoundException.class, e -> insertNew(m, tsPath)));

        return attempt
                .retryWhen(Retry.backoff(config.maxRetries, Duration.ofMillis(5))
                        .maxBackoff(Duration.ofMillis(500))
                        .filter(CouchbaseGuardedSinkWriter::isRetryable)
                        .doBeforeRetry(s -> casRetries.inc()));
    }

    private Mono<Void> writeIfNewer(Mutation m, String tsPath, LookupInResult current) {
        if (current.exists(0)) {
            long storedTs = current.contentAs(0, Long.class);
            if (m.ts <= storedTs) {
                skippedStale.inc();
                return Mono.empty(); 
            }
        }
        boolean sectionExists = current.exists(1);

        List<MutateInSpec> specs = new ArrayList<>(2);
        if (m.isDelete) {
            if (sectionExists) {
                specs.add(MutateInSpec.remove(m.section));
            }
        } else {
            specs.add(MutateInSpec.upsert(m.section, m.payload));
        }
        specs.add(MutateInSpec.upsert(tsPath, m.ts).createPath());

        return collection.mutateIn(m.docKey, specs, MutateInOptions.mutateInOptions().cas(current.cas()))
                .doOnSuccess(r -> written.inc())
                .then();
    }

    private Mono<Void> insertNew(Mutation m, String tsPath) {
        List<MutateInSpec> specs = new ArrayList<>(2);
        if (!m.isDelete) {
            specs.add(MutateInSpec.upsert(m.section, m.payload));
        }
        specs.add(MutateInSpec.upsert(tsPath, m.ts).createPath());

        return collection.mutateIn(m.docKey, specs,
                        MutateInOptions.mutateInOptions().storeSemantics(StoreSemantics.INSERT))
                .doOnSuccess(r -> written.inc())
                .then();
    }

    private static boolean isRetryable(Throwable t) {
        return t instanceof CasMismatchException
                || t instanceof DocumentExistsException
                || t instanceof TemporaryFailureException
                || t instanceof TimeoutException; 
    }

    static final class Mutation {
        final boolean isDelete;
        final String docKey;
        final String section;
        final JsonObject payload;
        final long ts;

        private Mutation(boolean isDelete, String docKey, String section, JsonObject payload, long ts) {
            this.isDelete = isDelete;
            this.docKey = docKey;
            this.section = section;
            this.payload = payload;
            this.ts = ts;
        }

        static Mutation fromRow(Row row) {
            try {
                String op = (String) row.getField(0);
                String docKey = (String) row.getField(1);
                String section = (String) row.getField(2);
                String json = (String) row.getField(3);
                Long ts = (Long) row.getField(4);

                if (docKey == null || section == null || ts == null || !SAFE_SECTION.matcher(section).matches()) {
                    LOG.warn("Invalid record: key={} section={} ts={}", docKey, section, ts);
                    return null;
                }
                boolean delete = "DELETE".equalsIgnoreCase(op);
                JsonObject payload = delete ? null : JsonObject.fromJson(json);
                return new Mutation(delete, docKey, section, payload, ts);
            } catch (RuntimeException e) {
                LOG.warn("Unparseable record: {}", row, e);
                return null;
            }
        }
    }
}