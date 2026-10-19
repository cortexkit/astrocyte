# Data model

astrocyte keeps one SQLite ledger at `<data home>/cortexkit/astrocyte/store.db`.
The schema is the `MIGRATIONS` chain in `crates/astrocyte-core/src/store.rs`,
currently version 24. A schema change appends a migration; an applied
migration is never edited. A binary refuses to serve a store whose schema is
ahead of its own chain (test: `store_ahead_of_binary_refuses_startup_by_name`).

## Tables

| Table | Holds | Writes |
|---|---|---|
| `usage_event` | Each row records one usage event per producer reference and lineage, together with its pricing projection. | insert; three projection columns, see below |
| `measurement` | Each row records one immutable event quantity for a role and unit. | insert only |
| `chargeability_transition` | Each row records the one terminal settlement of an event's chargeability. | insert only |
| `quarantine_evidence` | Each row preserves a referring event's quantities that contradict its target. | insert |
| `rejected_delivery` | Each row preserves a redelivery whose payload differs from the stored event. | insert |
| `redelivery_log` | Each row records an identical redelivery pushed by a peer. | insert |
| `adjustment` | Each row records a signed write-off, restatement, repricing, refund or goodwill adjustment. | insert |
| `priced_charge` | Each row records a charge-chain entry for a measurement and the origin of the pricing act. | insert only |
| `pricing_pending` | Each row identifies an event with a measurement that has no current charge head. | queue |
| `pricing_selection_state` | The row records the plan generation and the generation that coverage backfill has drained. | one row |
| `account_subscription` | Each row declares an effective-dated subscription plan for a provider account. | insert; close once |
| `statement` | Each row records a vendor statement identity. | insert only |
| `statement_line` | Each row preserves immutable statement line content and its current match projection. | insert; match projection |
| `match_attempt` | Each row records a statement-line match attempt and the amounts captured for that attempt. | insert only |
| `statement_import` | Each row records CSV import provenance and its content digest for idempotency. | insert only |
| `desired_config` | The row records the committed desired-configuration generation. | one row |
| `source_runtime` | Each row records a producer's cursor, lineage, last successful drain and pending disclosure. | upsert |
| `set_aside_record` | Each row records an unreadable producer record by source and locator, including its reason and observation times. | upsert |
| `capacity_runtime` | The row records whether the capacity axis is disabled, configured but unavailable, or active. | upsert |
| `capacity_window` | Each row records a provider account's latest QTA quota window. | upsert |
| `capacity_observation` | Each row records the latest capacity observation for an identity. | upsert |
| `capacity_provider_reading` | Each row records the latest capacity reading for a provider without account attribution. | upsert |
| `window_sample` | Each row preserves a capacity-window observation and its breakdown in the append-only history. | insert |
| `multiplier_observation` | Each row records a daily subscription multiplier observation or an operator recompute that supersedes an earlier observation. | insert only |
| `summary_value` | Each row assigns an exact stored string an integer identifier, with identifier zero reserved for null. | Builders insert new values. |
| `summary_hour` | Each row records an hour's persistent revision, published revision, format, event count and time extrema. | Triggers advance revisions, and publication records a completed build. |
| `summary_dirty_hour` | Each row identifies an hour that needs a fresh reduction from base rows. | Triggers insert dirty keys, and successful publication removes them. |
| `summary_cell` | Each row holds dimension-keyed event and measurement counters, exact money sums and reason maps for one hour. | Rebuilds replace these rows. |
| `summary_quantity` | Each row holds exact quantities for a cell, role, unit and exponent, including the covered population and presence counts. | Rebuilds replace these rows. |
| `summary_money` | Each row holds current-head or adjustment contributions in their actual currency, together with a presence count. | Rebuilds replace these rows. |
| `summary_cost_frequency` | Each row counts an exact current-head value in the dated, complete, currency-known work-class sample. | Rebuilds replace these rows. |
| `summary_audit_state` | The row records the independent audit's cursor, next start time and progress statistics. | The audit updates this row. |

## Immutability

Triggers abort the change with the trigger's name as the error. "Insert only"
in the table above means both of these exist:

- `usage_event_no_delete`, `measurement_no_update`, `measurement_no_delete`
- `chargeability_transition_no_update`, `chargeability_transition_no_delete`
- `priced_charge_no_update`, `priced_charge_no_delete`
- `statement_no_update`, `statement_no_delete`, `statement_line_no_delete`
- `match_attempt_no_update`, `match_attempt_no_delete`
- `statement_import_no_update`, `statement_import_no_delete`
- `multiplier_observation_no_update`, `multiplier_observation_no_delete`
- `account_subscription_no_delete`

`usage_event_update_guard` lets exactly three columns of an event move, all
projections the ledger maintains: `pricing_generation` advances by one per
pricing act, `charge_basis` changes only together with that step (a basis
correction), and `currency` moves from null to one value once. Every
producer field, including `auth_selection` and BROCA's charge-basis
provenance, is write-once. `statement_line_update_guard` keeps line content
fixed while the match projection moves. `account_subscription_update_guard`
allows one change: closing an open plan.

A correction to any of these tables is a new row, never an edit. Tests:
`usage_event_delete_is_refused_without_effect`,
`measurement_delete_is_refused_without_effect`,
`chargeability_transition_delete_is_refused_without_effect`,
`priced_charge_update_and_delete_are_refused_without_effect`,
`statement_content_and_history_are_immutable_without_effect`,
`storage_update_and_delete_guards_preserve_subscription_history`,
`schema_v13_provenance_columns_start_null_and_are_immutable`.

## Event identity

An event's natural key is `(producer, producer_lineage, producer_ref)`.
`event_id` is the SHA-256 of the key's canonical JSON. `producer_lineage` is
optional and its absence is part of the key's shape: an absent lineage omits
the field, so it never equals an empty-string lineage. A lineage separates
reference spaces that restart (a rebuilt producer store); a producer whose
references are already stable sends none. `producer_ref` is stored verbatim.
`auth_selection` is not part of the identity.

Two unique constraints enforce the key: the table constraint, and
`usage_event_natural_key_lineage_absent` for keys without a lineage, because
SQLite treats nulls as distinct.

A second delivery of an existing key is compared with the stored payload. An
identical payload is an exact redelivery and changes nothing (a pushed one
adds a `redelivery_log` row; a file re-read adds none). A different payload is
written to `rejected_delivery` and the stored event stays. Tests:
`identity_hash_bytes_match_persisted_format`,
`lineage_absent_and_empty_string_lineage_are_distinct_stored_events`,
`same_identity_different_payload_is_quarantined_without_overwrite`,
`read_exact_redelivery_writes_no_redelivery_log_row`.

`prior_event_ref` names an earlier event of the same producer and lineage.
When the referring event's quantities agree with its target, the pair settles
chargeability; a contradicting quantity becomes `quarantine_evidence`. A
reference whose target has not arrived waits for it.

## Measurements

A measurement is `(event_id, role, unit)` with `kind`, `source` and a
quantity `quantity_units × 10^quantity_exponent` (exponent -9 to 0). Both
quantity parts are present or both are absent. An absent quantity means the
producer reported the role but no usable number; it never prices and is never
zero. Token roles are disjoint: `input`, `cache_read`, `cache_write`,
`output`, `reasoning`. How each harness's counters map onto them is in
[harness readers](harness-readers.md).

## Charge basis

`charge_basis` is one of:

| Basis | Meaning | Pricing |
|---|---|---|
| `metered_api` | Billed per token at list price | Catalog rate; `priced` or `unpriced` |
| `subscription_included` | Covered by a subscription plan | `covered` at zero when a plan is in force at the event's instant; else `unpriced` with `no_subscription_in_force` |
| `unknown` | The producer cannot say who paid | `unpriced` with `charge_basis_unknown` |

Every harness reader records `unknown`, because no transcript says how the
call was paid for. An event with no `occurred_at_ms` is `unpriced` with
`undated_event` whatever its basis. BROCA states a basis per fact, and pushing peers state it
in `spend.event.ingest`. An operator changes an event's basis with
`spend.basis.correct`; see [operations](operations.md).

## The priced-charge chain

Each measurement has one chain of `priced_charge` rows. A row's `outcome` is
`priced` (has `currency` and `charge_nanos`), `covered` (zero, with the
plan's currency) or `unpriced` (has `unpriced_reason` and no money). The
`origin` names the act that wrote it: `pricing_pass`, `operator_reprice`,
`basis_correction`, or `unattributed` for rows written before origins were
recorded. Every row carries the event's `pricing_generation` and
`charge_basis` at the time it was written (trigger
`priced_charge_matches_event_generation_and_basis`).

A later row names the local integer `charge_key` it replaces in `supersedes`.
A scalar FK and `priced_charge_supersedes_same_measurement` insert trigger require
an existing predecessor on the same `(event_key, role, unit)`, even when FKs are
deferred. `supersedes` is unique, so a chain never forks. Event digests remain
TEXT served identities and dedup keys; every child stores the local `event_key`.
Charge digests are not stored. The first charge in the sequence has `supersedes IS NULL`; the partial
index `priced_charge_one_chain_root` allows one such row per measurement.

**The current head is the row no other row supersedes:**

```sql
SELECT * FROM priced_charge pc
WHERE NOT EXISTS (SELECT 1 FROM priced_charge child WHERE child.supersedes = pc.charge_key)
```

`supersedes IS NULL` selects each chain's original, not its head. Every
report, the verdict and pricing read heads with `NOT EXISTS`. Tests:
`only_the_charge_nothing_supersedes_counts_toward_verdict_and_total`,
`basis_correction_supersedes_every_head_preserving_every_prior_column`,
`mixed_reprice_preserves_priced_head_and_supersedes_only_unpriced_head`.

`pricing_pending` holds every event with a measurement that has no head. An
event is queued in the transaction that records it and dequeued in the
transaction that heads its last measurement. A periodic reconciliation
re-checks the queue against the full ledger.

`multiplier_observation` uses the same rule: a day's head is the row no other
row supersedes, and `multiplier_observation_one_original_per_day` allows one
original per quota provider and day. Test:
`pace_uses_the_latest_day_head_not_a_superseded_row`.

## Money

Money is a signed 64-bit integer count of nanounits (10^-9) of the row's
currency: nanodollars for USD. Corrections are signed deltas, so a total can
be negative. Arithmetic is checked in 128 bits. An overflow is a data error:
the measurement is stored `unpriced` with `arithmetic_out_of_range`, never
wrapped or capped. There is one rounding site, `round_nano_num` in
`crates/astrocyte-core/src/money.rs`, which rounds half to even. No
floating-point value touches money. Tests: `round_half_even_midpoints`,
`round_negative_midpoints_are_even_too`,
`signed_consumption_is_checked_after_refunds_cancel_large_heads`.

`priced_charge` amounts and rates are never negative (trigger
`priced_charge_nonnegative`); signed movement lives in `adjustment`. An
adjustment's currency must equal its event's
(`adjustment_currency_matches_event`). Adjustment reasons are `write_off`,
`vendor_restatement`, `our_repricing`, `refund`, `goodwill`; origins are
`statement`, `correction_ingest`, and `operator` in the schema.

## Chargeability

An event starts `unresolved`, which has no row. One transition moves it to a
terminal state, and the schema ties each state to who may set it:

| `to_state` | `resolved_by` |
|---|---|
| `billed` | `later_record`, `statement` |
| `written_off` | `correction`, `operator` |
| `unbillable` | `operator` |

The primary key on `event_id` makes the transition happen at most once.


## Hourly summaries

Migration 24 adds derived summary tables beside the ledger; it does not delete
usage events, measurements, charge history or adjustments. It creates empty
summary storage and marks every existing UTC hour, including the undated
partition, dirty so that the refresher can build history in the background.
Reports remain exact while that work is incomplete because they can still read
the original rows.

### Grain and exact values

A summary cell is keyed by its UTC hour, producer, provider, model, account,
auth selection, charge basis, work class, project and event currency. Hours use
floor division, including for timestamps before the Unix epoch. Undated events
have a separate partition rather than being assigned to the epoch or to their
recording time. The string dictionary preserves stored values without trimming,
case folding or aliasing; null has identifier zero, while an empty string has
its own positive identifier.

The cell payload records event and measurement populations, reason maps and
money sums. Quantities are kept separately by role, unit and observed decimal
exponent, with missing quantities distinguished from stated zero. These are raw
measurement quantities, not a claim that summing overlapping token roles gives
one disjoint billed-token count. Money and quantity sums use checked i128
arithmetic and are stored as decimal text, so SQLite never coerces them through
floating point. Presence counts preserve a genuine zero contribution and its
currency instead of treating it as absent. Contributions in different currencies
are never added together.

The event-priced count is determined before aggregation: an event needs at least
one measurement, a current head for every measurement, and no blocking declined
quantity. An examined, declined measurement whose stated quantity is zero does
not block that report verdict. A declined measurement with an absent or nonzero
quantity still blocks, as does any measurement that has no current head.
Budget uncertainty is a separate count because its existing rule is no heads or
any unpriced head, even when the declined quantity is zero. For populations too
large to prove that SQLite's signed limb sums cannot overflow, the verdict keeps
its original SQL path and error behavior.

Work-class percentiles use exact value frequencies, not fixed bins or averages
of hourly percentiles. Only dated, complete events with a known currency enter
the sample, and their value is the sum of their current priced heads before
adjustments. The reader merges frequencies and selects the same nearest rank
as the base-row report. Signed adjustments change spend and budget consumption,
but they do not change that head-only cost distribution.

### Transactional invalidation

Event insertion, pricing-header updates, adjustment insertion and chargeability
transitions mark the affected event's original hour dirty and advance its
persistent revision in the same SQLite transaction. An adjustment is attributed
to the usage event's occurrence hour, not to the adjustment's own timestamp.
Basis corrections, explicit repricing and subscription coverage backfill use
the same header-update path, so changes to heads, currency, reasons or the
metered subtotal invalidate the old cells together. A chargeability transition
is conservatively marked even though a transition alone does not remove money
from the existing consumption reports.

There is no dirty write for each measurement or charge row. Measurements are
inserted with their new event, and every production caller of
`pricing::insert_charge` first updates the event header in
`pricing::commit_reprice_in_tx`. The existing
`priced_charge_matches_event_generation_and_basis` guard rejects a head that
does not agree with the header's generation and basis, but it does not prove
that the header changed in the same transaction. Every future charge-writing
path must preserve that header-first transaction sequence. A refused write or a
rolled-back savepoint rolls back its dirty mark as well as its facts.

### Rebuilding and reading

`SummaryMaintenance` reads at most 5,000 events per source chunk in a short read
snapshot. It keeps a fresh staged reduction across chunks only while the hour's
revision remains unchanged. Publication takes the ordered writer transaction,
checks the revision again, replaces all derived rows for that hour and removes
the dirty key atomically. The persistent revision survives dirty-key removal,
so a later write cannot be mistaken for the revision that a builder read
earlier. No stored money, count or quantity total is maintained by applying
deltas to the previous summary.

The refresher runs this work after normal ingestion, pricing and pricing-queue
reconciliation. Refresh and audit share a 1.5-second soft maintenance allowance:
at least one chunk can run, and the observed slowest chunk guides whether
another will fit. This is not a hard elapsed-time guarantee for a slow SQLite
statement. Stop checks and SQLite interruption bound shutdown work, while
persistent failures back off for an hour. A build that exceeds the 50,000-row
publication ceiling stays dirty and continues to use live aggregation rather
than publishing a partial answer.

A report reads the manifest, dirty keys, summary rows and required base rows in
one caller-owned SQLite snapshot. It uses summaries only for complete, clean
whole hours whose published revision and format match. Dirty hours, missing
summaries and hours that cross a local-calendar or subscription-plan boundary
use the original event timestamps and base-row logic. This includes fractional
timezone offsets, daylight-saving changes and arbitrary plan effective times.
The two sources are disjoint, so no event is counted twice. When dirty coverage
is broad, or a derived value cannot be used safely, the reader retains the
original full base-row path.

Subscription plans and costs, ingest freshness, set-aside evidence and current
capacity are still read live rather than being frozen into the cube. Rate
staleness combines the exact minimum current-head resolution time with the
request's clock. Multiplier pace, multiplier reports and recomputes, and
per-event charge explanation keep their existing paths; raw hourly token sums
cannot replace their per-event valuation rules or history reads.

### Independent audit

The audit compares fresh reductions from base rows with published summaries,
independently of the dirty queue. It walks both base and manifest hours, so it
can discover an unmarked new hour that has no summary manifest. It also compares
counts, money, quantities, exact frequencies and metadata, treating malformed
derived data as a rebuild request rather than as zero money. A mismatch in a
clean hour is marked dirty before a warning is logged, allowing later reports
to use base rows while the refresher repairs it.

The audit persists its cursor and progress statistics, and its next pass starts
an hour after the previous pass completes. A full pass can take many ticks, so
this is an hourly interval between passes, not a promise to finish the whole
ledger every hour. Transactional invalidation keeps normal reports exact at
every snapshot; the audit is a backstop for a missed mark and cannot make an
earlier stale reply retroactively exact.

The implementation is in `crates/astrocyte-core/src/report/hourly.rs`, and the
refresher schedules it from `crates/astrocyte-module/src/refresher.rs`. The
original reducers remain available for fallback and byte-equivalence tests.
