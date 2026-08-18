# hitorro-mesh-pipelines-lucene

Apache Lucene backend for the `lucene` sink + source kinds in
[hitorro-mesh-pipelines](https://github.com/geekychris/hitorro-mesh-pipelines).
Drop this jar on the classpath and the pipelines'
`SinkRegistry` / `SourceFactory` auto-load the adapter via
`ServiceLoader` — `{kind: lucene, name: X}` in a job spec starts
writing docs to `${HITORRO_PIPELINES_HOME}/lucene/X/` with no extra
wiring.

## Wire shape

Sink (write path):

```yaml
sinks:
  - {kind: lucene, name: airports}          # storeSource defaults to true
  - {kind: lucene, name: raw,  storeSource: false}   # opt out for size
```

Source (search path):

```yaml
source: {kind: lucene, name: airports, query: "kennedy"}
```

## storeSource default

**Defaults to `true`.** A minimal spec `{kind: lucene, name: X}` will
retain the original JSON as a stored `_source` field so search hits
carry real content. Explicitly setting `false` opts into the storage-
lean mode — hits find the doc but return `{}` bodies (matching Lucene's
`Field.Store.NO` semantics on every field). Test coverage nails both
paths in `LuceneSearchServiceTest`.

## Exactly-once contract

`LuceneSink` overrides `Sink.addIdempotent(taskId, seq, row)` using
Lucene's native `IndexWriter.updateDocument(Term, doc)` — atomic
delete-by-term (on a hidden `_taskseq` StringField) + add-fresh. A
retried mapper that re-sends the same `(taskId, seq)` replaces the
prior doc instead of adding a duplicate hit. Storage overhead: one
small `StringField` per doc. Coverage in `LuceneSinkIdempotentTest`
(5 tests).

## Search service (REST-facing)

`LuceneSearchService` — the Spring-service shape the driver-app's
REST controller consumes. Exposes:

- `listIndexes()` → `[{name, docCount}]`
- `search(name, queryStr, limit)` → `{index, query, totalDocsInIndex,
  hitCount, tookMs, hits[]}` — hits are original JSON when
  `storeSource=true` was used at write time, otherwise reconstructed
  best-effort from stored fields.

`limit` is clamped to `[1, 1000]`; missing index name → 4xx clear
error. Coverage in `LuceneSearchServiceTest` (11 tests) including
list edge cases, quoted / dotted queries, `storeSource=false`
documented gotcha.
