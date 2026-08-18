/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.adapters.lucene;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.mesh.pipelines.model.SinkSpec;
import com.hitorro.mesh.pipelines.model.SourceSpec;
import com.hitorro.mesh.pipelines.sinks.SinkRegistry;
import com.hitorro.mesh.pipelines.sources.SourceFactory;
import com.hitorro.util.core.iterator.sinks.Sink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the exactly-once contract on {@link com.hitorro.util.core.iterator.sinks.index.LuceneSink}
 * ({@code Sink.addIdempotent}). Retried mappers must NOT produce
 * duplicate hits — the sink's implementation uses Lucene's native
 * {@code IndexWriter.updateDocument(Term, doc)} to atomically
 * delete-by-taskseq + add-fresh.
 */
class LuceneSinkIdempotentTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void sameTaskSeqTwice_indexHasExactlyOneDoc(@TempDir Path home) throws Exception {
        SinkRegistry reg = new SinkRegistry(home);
        try (Sink<JsonNode> sink = reg.create(new SinkSpec.Lucene("airports", true))) {
            sink.start();
            sink.addIdempotent("t-1", 42L,
                    JSON.readTree("{\"iata\":\"JFK\",\"name\":\"John F Kennedy — first\"}"));
            // Same (taskId, seq) — mapper retry. Body differs to prove
            // the SECOND version wins the delete-and-replace race
            // (Lucene's updateDocument semantics: delete-by-term then
            // add — the fresh doc lands).
            sink.addIdempotent("t-1", 42L,
                    JSON.readTree("{\"iata\":\"JFK\",\"name\":\"John F Kennedy — second\"}"));
        }

        // Search back — must return exactly ONE hit; content is the
        // second write (last-writer-wins).
        SourceFactory sf = new SourceFactory(reg);
        List<JsonNode> hits = drain(sf.open(new SourceSpec.Lucene("airports", null), new AtomicBoolean()));
        assertThat(hits).hasSize(1);
        assertThat(hits.get(0).get("name").asText()).contains("second");
    }

    @Test
    void differentSeqsSameTask_bothPersist() {
        // Not a retry — two genuine rows from the same task at different
        // sequence numbers. Both survive.
        try {
            SinkRegistry reg = new SinkRegistry(java.nio.file.Files.createTempDirectory("lucidem"));
            try (Sink<JsonNode> sink = reg.create(new SinkSpec.Lucene("multi", true))) {
                sink.start();
                sink.addIdempotent("t-1", 1L, JSON.readTree("{\"iata\":\"JFK\"}"));
                sink.addIdempotent("t-1", 2L, JSON.readTree("{\"iata\":\"LHR\"}"));
                sink.addIdempotent("t-1", 3L, JSON.readTree("{\"iata\":\"NRT\"}"));
            }
            SourceFactory sf = new SourceFactory(reg);
            List<JsonNode> hits = drain(sf.open(new SourceSpec.Lucene("multi", null), new AtomicBoolean()));
            assertThat(hits).hasSize(3);
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    @Test
    void differentTasksSameSeq_bothPersist(@TempDir Path home) throws Exception {
        // Two DIFFERENT tasks that happen to share seq — must not
        // collide, otherwise mapper A and mapper B would delete each
        // other's rows on parallel-shuffle jobs.
        SinkRegistry reg = new SinkRegistry(home);
        try (Sink<JsonNode> sink = reg.create(new SinkSpec.Lucene("crosstask", true))) {
            sink.start();
            sink.addIdempotent("t-a", 5L, JSON.readTree("{\"src\":\"a\"}"));
            sink.addIdempotent("t-b", 5L, JSON.readTree("{\"src\":\"b\"}"));
        }
        SourceFactory sf = new SourceFactory(reg);
        List<JsonNode> hits = drain(sf.open(new SourceSpec.Lucene("crosstask", null), new AtomicBoolean()));
        assertThat(hits).hasSize(2);
        assertThat(hits).extracting(r -> r.get("src").asText())
                .containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void nullTaskId_fallsBackToPlainAdd_noDedup(@TempDir Path home) throws Exception {
        // Base contract: null taskId → no dedup, at-least-once.
        // Sink writes both docs — behaves like plain add() twice.
        SinkRegistry reg = new SinkRegistry(home);
        try (Sink<JsonNode> sink = reg.create(new SinkSpec.Lucene("nulltask", true))) {
            sink.start();
            sink.addIdempotent(null, 1L, JSON.readTree("{\"n\":1}"));
            sink.addIdempotent(null, 1L, JSON.readTree("{\"n\":2}"));
        }
        SourceFactory sf = new SourceFactory(reg);
        List<JsonNode> hits = drain(sf.open(new SourceSpec.Lucene("nulltask", null), new AtomicBoolean()));
        assertThat(hits).hasSize(2);
    }

    @Test
    void countReflectsIdempotentWrites(@TempDir Path home) throws Exception {
        // sink.count() drives the UI's "rows written" indicator — must
        // stay honest across mixed add() / addIdempotent() calls.
        SinkRegistry reg = new SinkRegistry(home);
        try (Sink<JsonNode> sink = reg.create(new SinkSpec.Lucene("counted", true))) {
            sink.start();
            sink.add(JSON.readTree("{\"n\":1}"));
            sink.addIdempotent("t", 1L, JSON.readTree("{\"n\":2}"));
            sink.addIdempotent("t", 2L, JSON.readTree("{\"n\":3}"));
            sink.addIdempotent("t", 1L, JSON.readTree("{\"n\":\"2b\"}"));   // dedup replay
            // 4 calls total — sink.count reflects each call attempted;
            // physical distinct rows in the index = 3 (add + first 2
            // seqs; the replay updates the existing doc, not adds).
            assertThat(sink.count()).isEqualTo(4);
        }
        SourceFactory sf = new SourceFactory(reg);
        List<JsonNode> hits = drain(sf.open(new SourceSpec.Lucene("counted", null), new AtomicBoolean()));
        assertThat(hits).hasSize(3);
    }

    // ------------------------------------------------------------ helpers

    private static List<JsonNode> drain(Iterator<JsonNode> it) throws Exception {
        List<JsonNode> out = new ArrayList<>();
        try { while (it.hasNext()) out.add(it.next()); }
        finally { if (it instanceof AutoCloseable c) c.close(); }
        return out;
    }
}
