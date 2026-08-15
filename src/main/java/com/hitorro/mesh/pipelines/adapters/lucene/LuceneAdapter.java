/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.mesh.pipelines.adapters.lucene;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.mesh.pipelines.model.SinkSpec;
import com.hitorro.mesh.pipelines.model.SourceSpec;
import com.hitorro.mesh.pipelines.sinks.Sink;
import com.hitorro.mesh.pipelines.sinks.SinkAdapter;
import com.hitorro.mesh.pipelines.sources.SourceAdapter;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ServiceLoader-registered adapter for the {@code lucene} sink + source
 * kinds. Real Lucene index writes/reads override the Phase-1 NDJSON
 * stub whenever this jar is on the classpath.
 */
public final class LuceneAdapter implements SinkAdapter, SourceAdapter {

    @Override public boolean handles(SinkSpec spec)   { return spec instanceof SinkSpec.Lucene; }
    @Override public boolean handles(SourceSpec spec) { return spec instanceof SourceSpec.Lucene; }

    @Override
    public Sink create(SinkSpec spec, Path home) {
        SinkSpec.Lucene s = (SinkSpec.Lucene) spec;
        return new LuceneIndexSink(s.name(), s.storeSource(), home);
    }

    @Override
    public Iterator<JsonNode> open(SourceSpec spec, Path home, AtomicBoolean cancelled) throws Exception {
        SourceSpec.Lucene s = (SourceSpec.Lucene) spec;
        return new LuceneSearchSource(s.name(), s.query(), home);
    }
}
