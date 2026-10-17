# Data model

astrocyte keeps one SQLite ledger at `<data home>/cortexkit/astrocyte/store.db`.
The schema is the `MIGRATIONS` chain in `crates/astrocyte-core/src/store.rs`,
currently version 23. A schema change appends a migration; an applied
migration is never edited. A binary refuses to serve a store whose schema is
ahead of its own chain (test: `store_ahead_of_binary_refuses_startup_by_name`).

## Tables

| Table | Holds | Writes |
|---|---|---|
| `usage_event` | One usage event per natural key | insert; three projection columns, see below |
| `measurement` | The event's quantities, one row per `(role, unit)` | insert only |
| `chargeability_transition` | Whether an event is chargeable, settled once | insert only |
| `quarantine_evidence` | A referring event's quantities that contradict its target | insert |
| `rejected_delivery` | A redelivery whose payload differs from the stored event | insert |
| `redelivery_log` | An identical redelivery pushed by a peer | insert |
| `adjustment` | Signed money adjustments | insert |
| `priced_charge` | The priced-charge chain per measurement | insert only |
| `pricing_pending` | Events with a measurement that has no current head | queue |
| `pricing_selection_state` | Plan generation and the generation the coverage backfill drained | one row |
| `account_subscription` | Effective-dated subscription plans per provider account | insert; close once |
| `statement` | Vendor statement identities | insert only |
| `statement_line` | Statement line content | insert; match projection |
| `match_attempt` | Line-to-event matches with the amounts captured | insert only |
| `statement_import` | Import provenance and content digest | insert only |
| `desired_config` | The committed desired configuration | one row |
| `source_runtime` | Per-source cursor, lineage, last success and pending counts | upsert |
| `set_aside_record` | Unreadable producer records by source and locator | upsert |
| `capacity_runtime` | Capacity axis state | upsert |
| `capacity_window` | Latest quota windows per account | upsert |
| `capacity_observation` | Latest capacity observation per identity | upsert |
| `capacity_provider_reading` | Latest provider-level capacity readings | upsert |
| `window_sample` | Quota window history | insert |
| `multiplier_observation` | Daily subscription multiplier rows | insert only |

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
