/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.adapters.lucene;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.mesh.pipelines.model.SinkSpec;
import com.hitorro.mesh.pipelines.sinks.SinkRegistry;
import com.hitorro.util.core.iterator.sinks.Sink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the {@link LuceneSearchService} — the REST-facing query surface
 * that sits next to the pipeline sink at
 * {@code $HITORRO_PIPELINES_HOME/lucene/<name>/}. The sink half is
 * covered by {@link LuceneRoundTripTest}; this suite proves the
 * lookup / search / list paths a Spring controller invokes.
 *
 * <p>Two documents indexed per test — one with {@code storeSource=true}
 * (retains original JSON) and one without (index-only) — so the code
 * paths that reconstruct rows from stored fields vs. read {@code _source}
 * both get exercised.</p>
 */
class LuceneSearchServiceTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir Path home;
    LuceneSearchService svc;
    SinkRegistry sinks;

    @BeforeEach
    void setUp() {
        sinks = new SinkRegistry(home);
        svc   = new LuceneSearchService(home);
    }

    // -------------------------------------------------- listIndexes()

    @Test
    void listIndexes_empty_whenHomeMissing() {
        // Fresh home with no lucene/ subdir at all — must not crash.
        assertThat(svc.listIndexes()).isEmpty();
    }

    @Test
    void listIndexes_reportsNamesAndDocCounts() throws Exception {
        indexRows("airports", true,
                "{\"iata\":\"JFK\",\"name\":\"John F Kennedy\"}",
                "{\"iata\":\"LHR\",\"name\":\"London Heathrow\"}");
        indexRows("cities",   true,
                "{\"iso3\":\"USA\",\"name\":\"United States\"}");

        List<LuceneSearchService.IndexInfo> infos = svc.listIndexes();
        assertThat(infos).extracting(LuceneSearchService.IndexInfo::name)
                .containsExactly("airports", "cities");   // sorted alpha
        assertThat(infos).extracting(LuceneSearchService.IndexInfo::docCount)
                .containsExactly(2, 1);
    }

    @Test
    void listIndexes_nonIndexDirs_reportDocCountMinusOne() throws Exception {
        // A stray directory under lucene/ that isn't a real Lucene index —
        // must not crash the listing.
        Path root = home.resolve("lucene");
        Files.createDirectories(root.resolve("not-an-index"));
        List<LuceneSearchService.IndexInfo> infos = svc.listIndexes();
        assertThat(infos).hasSize(1);
        assertThat(infos.get(0).name()).isEqualTo("not-an-index");
        assertThat(infos.get(0).docCount()).isEqualTo(-1);
    }

    // -------------------------------------------------- search() happy path

    @Test
    void search_matchAll_whenQueryBlank() throws Exception {
        indexRows("airports", true,
                "{\"iata\":\"JFK\",\"name\":\"John F Kennedy\",\"country\":\"US\"}",
                "{\"iata\":\"LHR\",\"name\":\"London Heathrow\",\"country\":\"GB\"}",
                "{\"iata\":\"NRT\",\"name\":\"Tokyo Narita\",\"country\":\"JP\"}");

        var r = svc.search("airports", null,    20);
        assertThat(r.hitCount()).isEqualTo(3);
        assertThat(r.totalDocsInIndex()).isEqualTo(3);

        r = svc.search("airports", "  ",        20);   // blank → also match-all
        assertThat(r.hitCount()).isEqualTo(3);
    }

    @Test
    void search_fullTextQuery_hitsAnalysedText() throws Exception {
        indexRows("airports", true,
                "{\"iata\":\"JFK\",\"name\":\"John F Kennedy\"}",
                "{\"iata\":\"LHR\",\"name\":\"London Heathrow\"}");

        // The catch-all `_text` field is populated by the sink from every
        // stored value; the StandardAnalyzer lowercases so "heathrow"
        // matches even though the source has "Heathrow".
        var r = svc.search("airports", "heathrow", 20);
        assertThat(r.hitCount()).isEqualTo(1);
        assertThat(r.hits().get(0).get("iata").asText()).isEqualTo("LHR");
    }

    @Test
    void search_returnsSourceJson_whenStoreSourceTrue() throws Exception {
        // storeSource=true → hits carry the original JSON verbatim,
        // including nested fields the index doesn't project.
        indexRows("airports", true,
                "{\"iata\":\"NRT\",\"name\":\"Narita\",\"loc\":{\"lat\":35.7,\"lng\":140.4}}");

        var r = svc.search("airports", "narita", 20);
        assertThat(r.hitCount()).isEqualTo(1);
        JsonNode hit = r.hits().get(0);
        // Nested loc.lat survived — proves _source path, not the
        // "synthesise from stored fields" fallback.
        assertThat(hit.get("loc").get("lat").asDouble()).isEqualTo(35.7);
    }

    @Test
    void search_storeSourceFalse_returnsEmptyRowsButFindsHits() throws Exception {
        // Real behaviour + real gotcha: LuceneSink writes every field
        // with Field.Store.NO when storeSource=false, so search back
        // finds the doc (it IS indexed) but has no stored bytes to
        // reconstruct the row from — returns {}. Users who want content
        // retrieval via Lucene must set storeSource=true; the service
        // does NOT silently blow up in the other case, just returns
        // empty hit objects. Documenting the semantics via test so
        // a future refactor can't quietly change it.
        indexRows("airports", false,
                "{\"iata\":\"JFK\",\"name\":\"John F Kennedy\"}");

        var r = svc.search("airports", "kennedy", 20);
        assertThat(r.hitCount()).isEqualTo(1);
        // Doc found — the _text field is analysed, so "kennedy" matches
        // even though the raw field values weren't stored.
        JsonNode hit = r.hits().get(0);
        assertThat(hit.isObject()).isTrue();
        assertThat(hit.size()).isEqualTo(0);         // empty — nothing stored to reconstruct
    }

    // -------------------------------------------------- search() edge cases

    @Test
    void search_unknownIndex_throwsClearError() {
        assertThatThrownBy(() -> svc.search("does-not-exist", null, 20))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no such Lucene index");
    }

    @Test
    void search_limitClamped_lowBecomesDefault20() throws Exception {
        // limit <= 0 → 20; sanity: index 25 docs, request 0 → get 20.
        String[] rows = new String[25];
        for (int i = 0; i < 25; i++) rows[i] = "{\"i\":" + i + "}";
        indexRows("many", true, rows);

        var r = svc.search("many", null, 0);
        assertThat(r.hitCount()).isEqualTo(20);
        assertThat(r.totalDocsInIndex()).isEqualTo(25);
    }

    @Test
    void search_limitClamped_highCappedAt1000() throws Exception {
        // limit > 1000 → 1000. Index 5 docs → still returns 5 (the cap
        // is a ceiling, not a required floor).
        indexRows("small", true, "{\"n\":1}", "{\"n\":2}", "{\"n\":3}", "{\"n\":4}", "{\"n\":5}");
        var r = svc.search("small", null, 10_000);
        assertThat(r.hitCount()).isEqualTo(5);       // real hits, not capped
    }

    @Test
    void search_recordsTiming() throws Exception {
        indexRows("t", true, "{\"n\":1}");
        var r = svc.search("t", null, 5);
        assertThat(r.tookMs()).isGreaterThanOrEqualTo(0);
    }

    // ------------------------------------------------------------ helpers

    /** Runs a real pipeline SinkSpec.Lucene through SinkRegistry, so
     *  the on-disk shape is identical to what a real job produces. */
    private void indexRows(String name, boolean storeSource, String... rowsJson) throws Exception {
        try (Sink<JsonNode> sink = sinks.create(new SinkSpec.Lucene(name, storeSource))) {
            sink.start();
            for (String r : rowsJson) sink.add(JSON.readTree(r));
        }
    }
}
