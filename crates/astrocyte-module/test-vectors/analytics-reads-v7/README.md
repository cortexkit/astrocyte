# analytics-reads-v7

These samples come from the real `spend.report` dispatch over synthetic usage,
rates, subscriptions and attribution facts. They contain no live-store identity
or usage. Versions 3 through 6 remain unchanged.

Version 7 changes only `range-attribution-where.json` from version 6. The
row's `deduced.by_rule_share` is absent because subscription fees are no
longer assigned by rule. The rounded costs and all other sample bytes are
unchanged.

| Sample | Contract exercised |
| --- | --- |
| one-grouping, two-groupings, runner-harness | Producer, model and runner/host groupings; inclusive tokens and unknown quantities |
| hour, range, where-null | UTC hour buckets, outward-widened bounds and nullable filters |
| plan, plan-account, where-plan | List value and deduced cost using the unfiltered fee-allocation population |
| pending, no-usd-fees | Pending valuation disclosure and no synthetic USD total without USD fees |
| attribution-alfonso, attribution-parent, attribution-workload | Nullable owning agent, launching subject's owner and workload |
| attribution-project-id, attribution-workspace | Publisher project identity and dated workspace placement |
| attribution-alfonso-model, attribution-workload-parent | Attribution crossed with model, and two attribution dimensions |
| attribution-filter, attribution-null, attribution-unattributed | Attribution and model filters, nullable facts and genuinely unattributed calls |
| attribution-combination-legacy-project, attribution-combination-provider-model | Incompatible pairs refused with `combination_unavailable` |
| unavailable-grouping-task, unavailable-where-task | Deferred per-run task dimension |
| invalid-grouping-where, invalid-filter | Unknown dimension and empty allowed-value lists |
| invalid-range, invalid-range-missing-end | Equal/reversed or incomplete ranges |
| range-too-large, hour-no-range, hour-range-too-large | Calendar and widened-hour limits |
| hour-timezone-unsupported | Fractional-hour UTC offset refusal |
| malformed | Unknown request members |
| range-attribution-where | One range bucket, two attribution groupings and a model filter |
| range-ends-today | A range ending inside the current UTC day widens to the end of that day |
| reply-too-large | An error with size and row count when a day report exceeds the test budget by one byte |
| null-currency-deduced | Subscription-covered calls with zero recorded spend and null currency carry their USD fee share |

The last four cases pass through the reply adapter with a fixed clock of
1970-01-02T00:00:03.600Z. The earlier cases normalize only
`rate_staleness.as_of_ms` to zero.

`analytics_vectors_match_real_dispatch_and_sha256sums` pins compact JSON with
one trailing newline. To export the samples on the build server:

```sh
ASTROCYTE_EXPORT_ANALYTICS_VECTORS=1 cargo test --workspace --locked analytics_vectors_match_real_dispatch_and_sha256sums -- --nocapture
```

The test prints `VECTOR` JSON records with each filename and its exact content.
`SHA256SUMS` covers every JSON sample and this README, but not itself.
