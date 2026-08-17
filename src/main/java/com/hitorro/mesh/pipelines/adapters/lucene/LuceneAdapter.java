/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.adapters.lucene;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.mesh.pipelines.model.SinkSpec;
import com.hitorro.mesh.pipelines.model.SourceSpec;
import com.hitorro.mesh.pipelines.sinks.SinkAdapter;
import com.hitorro.mesh.pipelines.sources.SourceAdapter;
import com.hitorro.util.core.iterator.sinks.Sink;
import com.hitorro.util.core.iterator.sinks.index.LuceneSink;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ServiceLoader glue for the {@code lucene} sink + source kinds. The
 * actual sink implementation lives in {@code hitorro-index} as
 * {@link LuceneSink} — reusable outside the mesh pipeline runtime.
 */
public final class LuceneAdapter implements SinkAdapter, SourceAdapter {

    @Override public boolean handles(SinkSpec spec)   { return spec instanceof SinkSpec.Lucene; }
    @Override public boolean handles(SourceSpec spec) { return spec instanceof SourceSpec.Lucene; }

    @Override
    public Sink<JsonNode> create(SinkSpec spec, Path home) {
        SinkSpec.Lucene s = (SinkSpec.Lucene) spec;
        return new LuceneSink(s.name(), s.storeSource(), home);
    }

    @Override
    public Iterator<JsonNode> open(SourceSpec spec, Path home, AtomicBoolean cancelled) throws Exception {
        SourceSpec.Lucene s = (SourceSpec.Lucene) spec;
        Path indexDir = home.resolve("lucene").resolve(s.name());
        return new com.hitorro.util.core.iterator.sources.index.LuceneSearchSource(indexDir, s.query());
    }
}
