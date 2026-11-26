# analytics-reads-v5

These additions to the analytics read samples come from the real `spend.report`
dispatch and reply adapter over synthetic usage. Published versions 3 and 4
are unchanged. No live-store identity or usage is included.

| Sample | Contract exercised |
| --- | --- |
| range-attribution-where | One range bucket, two attribution groupings, and a model filter |
| range-ends-today | An end instant inside the fixture's current UTC day widens to the end of that day |
| reply-too-large | An Error with size and row count when a day report exceeds the test frame budget by one byte |

The fixtures use a fixed clock of 1970-01-02T00:00:03.600Z. The refusal uses a
smaller byte budget so the dispatch path can be checked without a large fixture.

`analytics_vectors_range_match_real_dispatch_and_sha256sums` pins compact JSON
with one trailing newline. To export the samples on the build server:

```sh
ASTROCYTE_EXPORT_ANALYTICS_VECTORS=1 cargo test --workspace --locked analytics_vectors_range_match_real_dispatch_and_sha256sums -- --nocapture
```

The test prints `VECTOR` JSON records with each filename and its exact content.
`SHA256SUMS` covers every sample and this README, but not itself.
