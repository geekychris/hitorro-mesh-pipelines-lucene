/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.adapters.lucene;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.mesh.pipelines.model.SinkSpec;
import com.hitorro.mesh.pipelines.model.SourceSpec;
import com.hitorro.mesh.pipelines.sinks.Sink;
import com.hitorro.mesh.pipelines.sinks.SinkRegistry;
import com.hitorro.mesh.pipelines.sources.SourceFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class LuceneRoundTripTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void index_via_sink_search_back_via_source(@TempDir Path home) throws Exception {
        SinkRegistry reg = new SinkRegistry(home);
        try (Sink sink = reg.create(new SinkSpec.Lucene("airports", true))) {
            sink.open();
            sink.add(JSON.readTree("{\"iata\":\"JFK\",\"name\":\"John F Kennedy\",\"country\":\"US\"}"));
            sink.add(JSON.readTree("{\"iata\":\"LHR\",\"name\":\"London Heathrow\",\"country\":\"GB\"}"));
            sink.add(JSON.readTree("{\"iata\":\"NRT\",\"name\":\"Tokyo Narita\",\"country\":\"JP\"}"));
            assertThat(sink.count()).isEqualTo(3);
        }
        // Physical index files exist.
        assertThat(home.resolve("lucene/airports/segments_1")).exists();

        SourceFactory sf = new SourceFactory(reg);

        // Match all.
        List<JsonNode> all = drain(sf.open(new SourceSpec.Lucene("airports", null), new AtomicBoolean()));
        assertThat(all).hasSize(3);
        assertThat(all).extracting(r -> r.get("iata").asText())
                .containsExactlyInAnyOrder("JFK", "LHR", "NRT");

        // Term match via analysed _text catch-all — "heathrow" tokenised
        // (lowercased) matches London Heathrow's stored name.
        List<JsonNode> heathrow = drain(sf.open(new SourceSpec.Lucene("airports", "heathrow"), new AtomicBoolean()));
        assertThat(heathrow).hasSize(1);
        assertThat(heathrow.get(0).get("iata").asText()).isEqualTo("LHR");
    }

    private static List<JsonNode> drain(Iterator<JsonNode> it) throws Exception {
        List<JsonNode> out = new ArrayList<>();
        try { while (it.hasNext()) out.add(it.next()); }
        finally { if (it instanceof AutoCloseable c) c.close(); }
        return out;
    }
}
