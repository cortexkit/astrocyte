# analytics-reads-v4

Tagged sample set: `analytics-reads-v4`. These frames come from the real
`spend.report` dispatch handler over synthetic usage, rates, subscriptions and
attribution facts. They contain no live-store identity or usage. The publisher
facts follow the contract; they are not samples of prefrontal's implementation.

All 20 non-attribution frames from version 3 remain byte-identical, including
list value, deduced cost and pending valuations. The generation test compares
them with the published version 3 files as well as pinning the version 4 bytes.
Version 3 remains immutable. The previously unavailable attribution dimensions
now have successful examples; `task` still refuses `dimension_unavailable`.

| Samples | Contract exercised |
| --- | --- |
| one-grouping, two-groupings, runner-harness | Ordinary producer, model and write-once runner/host groupings |
| hour, range, where-null | UTC hour buckets, outward-widened ranges and nullable filters |
| plan, plan-account, where-plan | List value and deduced subscription cost using the unfiltered fee-allocation population |
| pending, no-usd-fees | Pending valuation disclosure and no synthetic USD total without USD fees |
| attribution-alfonso, attribution-parent | Nullable owning agent and the launching subject's owning agent |
| attribution-workload | The publisher's open workload name |
| attribution-project-id, attribution-workspace | Publisher project identity and its dated placement, distinct from ledger `project` |
| attribution-alfonso-model, attribution-workload-parent | Attribution crossed with model, and two attribution dimensions |
| attribution-filter, attribution-null | Attribution and model filters; coverage ignores attribution filters |
| attribution-unattributed | A genuine no-fact population grouped as null, with unattributed call counts |
| attribution-combination-legacy-project, attribution-combination-provider-model | Refused incompatible pairs, named by `combination_unavailable` |
| unavailable-grouping-task, unavailable-where-task | Deferred per-run task dimension |
| invalid-grouping-where, invalid-filter | Unknown dimension and empty allowed-value lists |
| invalid-range, invalid-range-missing-end | Equal/reversed or incomplete ranges |
| range-too-large, hour-no-range, hour-range-too-large | Calendar and widened-hour limits |
| hour-timezone-unsupported | Fractional-hour UTC offset refusal |
| malformed | Unknown request members |

The original pending-usage fixture has no attribution facts. Attribution
success cases use a separate valued subscription fixture with an attributed
parent and child and one project placement. The child's `alfonso` is null even
though its fact, workload and parent are known. It therefore differs from the
no-fact population in `attribution-unattributed`.

The clock-dependent `rate_staleness.as_of_ms` alone is normalised to zero.
`analytics_vectors_match_real_dispatch_and_sha256sums` pins compact JSON frames
with one trailing newline. To export them on the build server:

```sh
ASTROCYTE_EXPORT_ANALYTICS_VECTORS=1 cargo test --workspace --locked analytics_vectors -- --nocapture
```

The test prints `VECTOR` JSON records with each filename and its exact content.
`SHA256SUMS` covers every JSON frame and this README, but not itself. This tag
versions reply examples, not the internal summary format.
