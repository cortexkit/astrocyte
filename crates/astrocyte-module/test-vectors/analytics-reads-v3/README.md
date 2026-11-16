# analytics-reads-v3

Tagged sample set: `analytics-reads-v3`. Version 3 retains version 2's sample
bodies apart from the additive list-value and deduced-cost members, and adds the
five cases below. Version 2's published tag remains the historical sample set.
These spend.report frames come from the real dispatch handler over invented
usage. No live-store identity or usage is included.

The existing cases and `no-usd-fees` use the original pending-usage fixture.
`plan`, `plan-account` and `where-plan` use a separate USD subscription fixture
with valued included usage; `pending` uses the same subscription usage with one
hour not yet valued. The clock-dependent `rate_staleness.as_of_ms` alone is
normalized to zero. No sample has a resolved recorded-charge rate timestamp,
so the oldest rate and its age are absent.

| Samples | Contract exercised |
| --- | --- |
| one-grouping, two-groupings | Producer and model groupings; inclusive output, cache lifetimes, exact media units and unknown quantity disclosure |
| runner-harness | Write-once runner labels crossed with the host harness; null means unmanaged |
| plan | Plan-pool grouping, USD API list value and deduced subscription cost, independent of zero recorded spend |
| plan-account | Plan pool crossed with the ledger account that supplies coverage |
| where-plan | Plan-pool filtering with model rows and subscription fee shares |
| pending | An unvalued hour retains recorded usage under plan `pending` and discloses `not_yet_valued_hours` and pending fee days |
| no-usd-fees | Null-currency usage without USD fees does not synthesize a USD total |
| hour | UTC hour buckets with an explicit bounded range |
| where-null | OR within a dimension, AND across dimensions, and JSON null for absent work class |
| range | Outward widening of a half-open range to whole local calendar buckets |
| invalid-grouping-where, invalid-filter | Unknown dimensions and empty allowed-value lists |
| invalid-range, invalid-range-missing-end | Reversed/equal range edges and incomplete range pairs |
| range-too-large, hour-no-range, hour-range-too-large | Calendar and widened-hour span bounds |
| hour-timezone-unsupported | Fractional-hour UTC offset refusal |
| unavailable-grouping-*, unavailable-where-* | The dimension names `alfonso`, `workload`, `workspace` and `parent_alfonso` are recognized but unavailable |
| malformed | Unknown request members are rejected |

`analytics_vectors_match_real_dispatch_and_sha256sums` regenerates and pins the
compact JSON frames (one trailing newline). To export newly generated frames:

```sh
ASTROCYTE_EXPORT_ANALYTICS_VECTORS=1 cargo test --workspace --locked analytics_vectors -- --nocapture
```

The test prints `VECTOR` JSON records containing each filename and exact bytes.
`SHA256SUMS` covers every JSON sample and this README, but not itself. The sample
tag versions the reply examples, not the report's internal cache format.
