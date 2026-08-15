/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.adapters.lucene;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Lightweight query service for pipeline-indexed Lucene indexes. Sits
 * next to the {@link LuceneIndexSink} — both address the same physical
 * index directory ({@code $HITORRO_PIPELINES_HOME/lucene/&lt;name&gt;}) so a
 * pipeline that ends in {@code {kind: lucene, name: X, storeSource:
 * true}} can be queried immediately via REST without any extra wiring.
 *
 * <p>Full-fat search runtime — the hitorro-index {@code IndexManager}
 * with {@code JVSLuceneIndexWriter}-driven type projection is the next
 * iteration when we wire the Type system through. This service handles
 * the "index arbitrary rows + query them back with fielded / full-text
 * SEARCH" case that works for every pipeline today.</p>
 */
public final class LuceneSearchService {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final Path home;

    public LuceneSearchService(Path home) {
        this.home = home;
    }

    /** List every named Lucene index that exists on-disk under this home. */
    public List<IndexInfo> listIndexes() {
        Path root = home.resolve("lucene");
        List<IndexInfo> out = new ArrayList<>();
        if (!Files.isDirectory(root)) return out;
        try (var stream = Files.list(root)) {
            stream.filter(Files::isDirectory).forEach(dir -> {
                try (FSDirectory fs = FSDirectory.open(dir);
                     DirectoryReader r = DirectoryReader.open(fs)) {
                    out.add(new IndexInfo(dir.getFileName().toString(), r.numDocs()));
                } catch (IOException ignored) {
                    out.add(new IndexInfo(dir.getFileName().toString(), -1));
                }
            });
        } catch (IOException ignored) { }
        out.sort((a, b) -> a.name().compareTo(b.name()));
        return out;
    }

    /**
     * Run a full-text query against a named index. Empty / null query →
     * MatchAllDocs.
     *
     * @param indexName the {@code name} you used in the pipeline sink spec
     * @param queryStr  Lucene query syntax over the synthetic {@code _text}
     *                  catch-all field, or exact field lookups like
     *                  {@code iata:LHR}
     * @param limit     max hits
     * @return one JsonNode per hit — the stored {@code _source} JSON if the
     *         sink used {@code storeSource: true}, otherwise a synthesised
     *         row of the stored fields
     */
    public SearchResult search(String indexName, String queryStr, int limit) throws Exception {
        Path dir = home.resolve("lucene").resolve(indexName);
        if (!Files.isDirectory(dir)) {
            throw new IllegalArgumentException("no such Lucene index: " + indexName);
        }
        int cap = Math.max(1, Math.min(1000, limit <= 0 ? 20 : limit));
        try (FSDirectory fsDir = FSDirectory.open(dir);
             DirectoryReader reader = DirectoryReader.open(fsDir)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            Query q = (queryStr == null || queryStr.isBlank())
                    ? new MatchAllDocsQuery()
                    : new QueryParser("_text", new StandardAnalyzer()).parse(queryStr);
            long t0 = System.nanoTime();
            ScoreDoc[] hits = searcher.search(q, cap).scoreDocs;
            long tookMs = (System.nanoTime() - t0) / 1_000_000;
            List<JsonNode> rows = new ArrayList<>(hits.length);
            for (ScoreDoc sd : hits) {
                Document d = searcher.storedFields().document(sd.doc);
                String src = d.get("_source");
                if (src != null) rows.add(JSON.readTree(src));
                else {
                    ObjectNode row = JSON.createObjectNode();
                    for (IndexableField f : d.getFields()) {
                        if (f.stringValue() != null) row.put(f.name(), f.stringValue());
                    }
                    rows.add(row);
                }
            }
            return new SearchResult(indexName, queryStr, reader.numDocs(), rows.size(), tookMs, rows);
        }
    }

    public record IndexInfo(String name, int docCount) { }

    public record SearchResult(String index, String query, int totalDocsInIndex,
                               int hitCount, long tookMs, List<JsonNode> hits) { }
}
