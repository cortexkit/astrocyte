# analytics-reads-v1

Tagged sample set: `analytics-reads-v1`. These spend.report frames come from the
real dispatch handler over an invented store. No live-store identity or usage is
included. The clock-dependent `rate_staleness.as_of_ms` alone is normalized to
zero; all measurements are pending, so the oldest rate and its age are absent.

| Samples | Contract exercised |
| --- | --- |
| one-grouping, two-groupings | Producer and model groupings; inclusive output, cache lifetimes, exact media units and unknown quantity disclosure |
| hour | UTC hour buckets with an explicit bounded range |
| where-null | OR within a dimension, AND across dimensions, and JSON null for absent work class |
| range | Outward widening of a half-open range to whole local calendar buckets |
| invalid-grouping-where, invalid-filter | Unknown dimensions and empty allowed-value lists |
| invalid-range, invalid-range-missing-end | Reversed/equal range edges and incomplete range pairs |
| range-too-large, hour-no-range, hour-range-too-large | Calendar and widened-hour span bounds |
| hour-timezone-unsupported | Fractional-hour UTC offset refusal |
| unavailable-grouping-*, unavailable-where-* | Alfonso, workload, workspace and parent Alfonso are recognized but unavailable |
| malformed | Closed request decoding |

`analytics_vectors_match_real_dispatch_and_sha256sums` regenerates and pins the
compact JSON frames (one trailing newline). To export newly generated frames:

```sh
ASTROCYTE_EXPORT_ANALYTICS_VECTORS=1 cargo test --workspace --locked analytics_vectors -- --nocapture
```

The test prints `VECTOR` JSON records containing each filename and exact bytes.
`SHA256SUMS` covers every JSON sample and this README, but not itself. The sample
tag is not a summary-cache version: schema migration 49 publishes summary format
5 so missing quantities cannot silently become measured zeros.
