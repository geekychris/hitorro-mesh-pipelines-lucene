/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.adapters.lucene;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.mesh.pipelines.sinks.Sink;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Real Lucene-backed sink. Each incoming row becomes one Document.
 * Fields:
 *
 * <ul>
 *   <li>Every scalar top-level field is added as a
 *       {@link StringField} (indexed, not analysed) — good for
 *       exact-match queries and terms aggregations.</li>
 *   <li>A {@code _text} catch-all is added as a {@link TextField}
 *       so users can run full-text queries without knowing field
 *       names.</li>
 *   <li>When {@code storeSource} is true, the entire row JSON is
 *       stored as a {@link StoredField} named {@code _source},
 *       so downstream searches can return the original row
 *       verbatim.</li>
 * </ul>
 *
 * <p>Physical index lands at {@code &lt;home&gt;/lucene/&lt;name&gt;/}.
 * {@link #close} commits and releases the writer.</p>
 */
public final class LuceneIndexSink implements Sink {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String name;
    private final boolean storeSource;
    private final Path dir;
    private final AtomicLong n = new AtomicLong();
    private FSDirectory fsDir;
    private IndexWriter writer;

    public LuceneIndexSink(String name, boolean storeSource, Path home) {
        this.name        = name;
        this.storeSource = storeSource;
        this.dir         = home.resolve("lucene").resolve(name);
    }

    @Override
    public void open() throws Exception {
        Files.createDirectories(dir);
        fsDir = FSDirectory.open(dir);
        IndexWriterConfig cfg = new IndexWriterConfig(new StandardAnalyzer())
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        writer = new IndexWriter(fsDir, cfg);
    }

    @Override
    public void add(JsonNode row) throws Exception {
        if (writer == null) open();
        Document doc = new Document();
        StringBuilder text = new StringBuilder();
        if (row.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> it = row.fields();
            while (it.hasNext()) {
                var e = it.next();
                if (e.getValue().isNull()) continue;
                if (e.getValue().isValueNode()) {
                    doc.add(new StringField(e.getKey(), e.getValue().asText(), Field.Store.NO));
                    text.append(e.getValue().asText()).append(' ');
                }
            }
        }
        doc.add(new TextField("_text", text.toString(), Field.Store.NO));
        if (storeSource) {
            doc.add(new StoredField("_source", JSON.writeValueAsString(row)));
        }
        writer.addDocument(doc);
        n.incrementAndGet();
    }

    @Override public long count() { return n.get(); }

    @Override
    public void close() throws Exception {
        try { if (writer != null) writer.commit(); } catch (Exception ignored) { }
        try { if (writer != null) writer.close(); } catch (Exception ignored) { }
        try { if (fsDir != null) fsDir.close(); }   catch (Exception ignored) { }
    }
}
