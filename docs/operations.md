# Operations

This guide covers the operator acts that change the ledger: declaring and
closing subscriptions, repricing an event, correcting its charge basis, and
recomputing a multiplier day. Each section says what the act writes, how to
verify it, and how to reverse it. Several of these acts are one-way; each
section says so. The operations and their refusal codes are specified in the
[module surface](module-surface.md); the tables are in the
[data model](data-model.md).

## 1. The management client

`spend_drive` calls one served operation and prints the reply. Build it from
this repository:

```sh
cargo build --locked -p astrocyte-module --bin spend_drive
```

Run it with the subc connection file the operator's clients use, the
operation name and its params as one JSON object:

```sh
target/debug/spend_drive <connection-file> <operation> '<json params>'
```

| Exit | Meaning | Output |
|---|---|---|
| 0 | The operation succeeded. | The reply as one JSON line on stdout. |
| 1 | The module refused, the transport failed, or the call timed out after 15 seconds. | `<code>: <message>` on stderr; a transport failure has code `transport`. |
| 2 | Wrong argument count, or the params are not valid JSON. | A usage line on stderr. |

`spend_drive` never retries. A timeout means the outcome of a write is
unknown: check with the verification step below before sending it again.
Tests: `refusal_exits_nonzero_with_code_and_message_on_stderr`,
`stale_correlation_reply_is_ignored_before_real_refusal`.

The verification steps also read the ledger directly. Open it read-only,
never for writing:

```sh
sqlite3 'file:<data home>/cortexkit/astrocyte/store.db?mode=ro'
```

A charge's current head is the row no other row supersedes. This query lists
an event's heads:

```sql
SELECT role, unit, outcome, charge_basis, charge_nanos, unpriced_reason, origin
FROM priced_charge pc
WHERE pc.event_key = (SELECT event_key FROM usage_event WHERE event_id = '<event_id>')
  AND NOT EXISTS (SELECT 1 FROM priced_charge child WHERE child.supersedes = pc.charge_key);
```

The `spend.charge.explain` request takes `event_id`. Within that event,
`(role, unit, pricing_generation)` identifies a charge. Replies no longer contain
`charge_id`; `supersedes` is the predecessor's pricing generation, or null for an
original. Local integer keys are never returned and must not be used as durable
external identities.

### Offline schema-23 rebuild

Stop all readers and writers, retain a rollback backup and reserve space for old
and new tables, the journal and a later VACUUM output (allow at least 50 GiB for a
10 GiB ledger). The new binary applies each pending migration in its own
transaction: first through 22 with FK enforcement on, then the schema-23 rebuild.
The migration checks that every child event reference and predecessor belongs to
the right event/measurement.
Orphaned event references, missing/cross-measurement predecessors, rebuilt FK
violations or row-count mismatches abort the rebuild with the failed check's name,
leaving schema 22 intact.
Foreign keys are suspended only on the startup writer during migration 23;
rebuilt-table FK checks run before commit, and enforcement is restored on success,
error or unwinding. Run SQLite integrity and whole-store FK checks offline before
and after the maintenance window. The rebuild retains event digests and all ledger
history but removes charge digests.
After the migration commits, stop the binary and run `VACUUM` on the offline store;
VACUUM is not part of the migration. Check row counts, current heads, head money,
`PRAGMA foreign_key_check` and `PRAGMA integrity_check` before restarting service.
Take a new full backup baseline: rebuilds and VACUUM relocate pages. Keep the old
file and matching binary for rollback; old binaries cannot read schema 23.

### SQLite temporary files

The store's writer keeps SQLite statement journals in memory, so ledger writes go
only to the store directory. Two kinds of SQLite work still use SQLite's
temporary directory: the report reader, when a large sort or grouping spills,
and the OpenCode reader, which queries OpenCode's own database. On Linux and
macOS, SQLite picks that directory from `SQLITE_TMPDIR`, then `TMPDIR`, then
`/var/tmp`; on Windows, from the system temporary path. It does not follow the
store's location. On Linux and macOS, set `SQLITE_TMPDIR` in the module's
environment to keep that work on another drive.

## 2. Subscriptions

A subscription plan makes events with charge basis `subscription_included`
for its `(provider, account_ref)` price as `covered` at zero while the plan is
in force. Without a plan in force they stay unpriced with
`no_subscription_in_force`.

### Declare a plan

```sh
spend_drive <connection-file> spend.subscription.set '{"account_ref":"<account>","provider":"anthropic","plan_name":"max","recurring_cost_minor":20000,"currency":"USD","amount_exponent":2,"period":"month","effective_from":<unix ms>}'
```

This inserts an `account_subscription` row and bounds any open plan for the
same key at `effective_from`. If `effective_from` is in the past, the reply
has `backdated: true` and a `coverage_notice`. The refresher then reprices,
a bounded number per tick, every unpriced head with reason
`no_subscription_in_force` whose event falls inside the plan's range.

Verify:

- `spend.subscription.report` with `{}` lists the plan.
- `spend.charge.explain` with `{"event_id": "<event_id>"}` for a covered
  event shows `covered` heads at zero, origin `pricing_pass`.
- `spend.ledger.coverage` with `{}`: `unpriced_events_by_reason` for
  `no_subscription_in_force` falls as the backfill drains.

Tests: `declaring_a_plan_reprices_the_event_it_covers_to_covered_at_zero`,
`coverage_backfill_runs_after_a_plan_write_and_not_on_a_quiet_tick`.

### Close a plan

```sh
spend_drive <connection-file> spend.subscription.close '{"account_ref":"<account>","provider":"anthropic","effective_to":<unix ms>}'
```

This sets the open plan's `effective_to`. Events after that instant stop
resolving to covered. Verify with `spend.subscription.report`. Test:
`closing_a_plan_bounds_it_and_later_events_stop_resolving_to_covered`.

### Reversing: coverage is one-way

Plans cannot be deleted or reopened, and a closed plan's end cannot move. A
covered head is a priced head, and neither the backfill nor
`spend.event.reprice` re-resolves priced heads. So:

- Closing or superseding a plan does not uncover any event it already
  covered, including events after `effective_to` that were priced while the
  plan was open. Test:
  `an_event_covered_before_the_close_stays_covered_afterwards`.
- A plan declared by mistake can be bounded with `spend.subscription.close`
  (at any instant after its `effective_from`), and a corrected plan declared
  from that instant onward. Events the mistaken plan covered stay covered.
- To uncover a specific event, correct its charge basis (section 4), which
  prices every measurement again under the new basis.

## 3. Reprice an event

```sh
spend_drive <connection-file> spend.event.reprice '{"event_id":"<event_id>"}'
```

This re-resolves every measurement whose head is unpriced or missing,
against the catalog or the subscription in force, and appends new heads with
origin `operator_reprice`. Priced and covered heads are kept; the reply
counts `measurements_repriced` and `measurements_preserved`. Use it after the
cause of an unpriced reason is fixed: a catalog that was not configured, a
model the catalog has since added. The refresher reprices queued events by
itself; this act is for one event now.

Verify with `spend.charge.explain` or the head query in section 1: each
measurement's charge marked `current: true` has the new outcome and origin
`operator_reprice`. `already_priced` means
every head is already priced or covered and nothing was written. Test:
`op_event_reprice_routes_subscription_included_through_coverage`.

Reversing: one-way. A priced head is never returned to unpriced, and the
superseded rows stay in the chain. To change the money on a priced event,
append an adjustment with `spend.correction.ingest` (see
[money ingest](money-ingest.md)) or correct its basis.

## 4. Correct an event's charge basis

Every harness event is recorded with basis `unknown`, so it stays unpriced
until its basis is known. When the operator knows how a call was paid:

```sh
spend_drive <connection-file> spend.basis.correct '{"event_id":"<event_id>","corrected_basis":"metered_api","reason":"<why>"}'
```

`corrected_basis` is `metered_api`, `subscription_included` or `unknown`.
This advances the event's `pricing_generation`, changes its `charge_basis`,
and appends a new head for every measurement with origin `basis_correction`,
replacing priced and covered heads too. `basis_unchanged` means the event
already has that basis.

Verify with `spend.charge.explain`: `event.charge_basis` is the new basis and
every head has origin `basis_correction`. Tests:
`basis_correction_supersedes_every_head_preserving_every_prior_column`,
`basis_correction_op_reaches_all_three_arms`.

Reversing: correct the basis back with another `spend.basis.correct`. That
is a new correction, priced against the catalog and plans as they are now;
the earlier heads stay in the chain as history. The event's currency, once
set, never changes, so a correction whose new price is in another currency is
refused `catalog_currency_mismatch` without a write.

## 5. Recompute a multiplier day

The refresher's daily multiplier stage records one row per quota provider per
UTC day.
To recompute a day with the current configuration and catalog, for example
after fixing a scope rule or a plan price:

```sh
spend_drive <connection-file> spend.multiplier.recompute '{"observation_day":"2026-10-01","quota_provider":"claude"}'
```

Omit `quota_provider` to recompute every configured provider in one
transaction. Each new row supersedes the day's head; a day with no row gets
its original. Recompute needs a connected fusiform price catalog
(`fusiform_connection_file`).

Verify with `spend.multiplier.report` over a range that includes the day:
the day's entry in `series` is the new row, and the reply's `observation_id`
and `supersedes` name the new and replaced rows. Test:
`a_recompute_supersedes_the_day_and_the_report_serves_it`.

Reversing: one-way. A superseded row can never become the head again, and
the daily multiplier stage never writes a second row for a day that has one.
Restoring the earlier result means restoring the earlier configuration and
recomputing again, which appends another row.

## 6. Check ledger coverage

```sh
spend_drive <connection-file> spend.ledger.coverage '{}'
```

This read writes nothing. It reports how many events are unpriced or
undated, unpriced counts by reason for events and for measurements, records
set aside per source, ingest freshness, rate staleness and capacity state.
Use it before and after each act above to confirm the change moved the count
you expected. There is nothing to reverse.
