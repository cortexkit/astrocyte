# Architecture

Code is authoritative; `STRUCTURE.md` maps it.

## System shape

Astrocyte (`ck-astrocyte`) records machine AI usage off the request hot path,
prices signed nanodollars against fusiform and serves reports/reconciliation over
subc. `budget.verdict` returns `allow`; cap enforcement is not built.

Three crates:

- `astrocyte-core`: identity, ingest, pricing, subscriptions, statements, reports,
  reconciliation and SQLite; no subc dependency.
- `astrocyte-module`: binary, serving/configuration, refresher, subc clients and mappings.
- `astrocyte-readers`: parsers for coding-harness session stores (Claude Code,
  Codex, OpenCode, Pi, Gemini CLI, Goose, Oh My Pi, Kimi CLI) and provider identity
  normalization.

## Durable state

`crates/astrocyte-core/src/store.rs` defines `MIGRATIONS` (versions 1 to 24).
`initialize_connection` checks foreign keys, probes JSON/triggers/partial indexes
and sets a 5000 ms busy timeout. Read-only connections copy the timeout, skipping
the table-creating probe.

After version 24 these tables exist:

| Table | Holds |
|---|---|
| `usage_event` | producer identity, immutable facts and pricing projection |
| `measurement` | immutable quantities by event, role and unit |
| `chargeability_transition` | append-only chargeability settlement |
| `quarantine_evidence` | contradictory referring quantities |
| `rejected_delivery` | conflicting redelivery evidence |
| `set_aside_record` | unreadable source/locator evidence and observation times |
| `redelivery_log` | identical pushed redeliveries |
| `adjustment` | signed write-off, restatement, repricing, refund or goodwill |
| `priced_charge` | immutable per-measurement charge chains and origins |
| `pricing_pending` | events with headless measurements |
| `pricing_selection_state` | plan/backfill generations |
| `account_subscription` | effective-dated subscription plans per provider account |
| `statement` | vendor statement identities |
| `statement_line` | immutable statement line content |
| `match_attempt` | append-only line matches and captured amounts |
| `statement_import` | CSV import provenance and content idempotency |
| `desired_config` | the committed desired-configuration generation |
| `source_runtime` | producer cursor, lineage, success and pending state |
| `capacity_runtime` | disabled, unavailable or active capacity axis |
| `capacity_window` | latest QTA quota windows per account |
| `capacity_observation` | latest capacity observation per identity |
| `capacity_provider_reading` | latest provider-level capacity readings |
| `window_sample` | append-only capacity window history and breakdown |
| `multiplier_observation` | immutable provider/day observations and superseding recomputes |
| `summary_value` | lossless string/null dictionary |
| `summary_hour` | persistent revisions, publication state and time extrema |
| `summary_dirty_hour` | hours requiring fresh reduction |
| `summary_cell` | dimension-keyed counts, exact money and reason maps |
| `summary_quantity` | role/unit/exponent quantities and covered populations |
| `summary_money` | actual-currency contributions and presence counts |
| `summary_cost_frequency` | exact per-event head-value frequencies |
| `summary_audit_state` | independent audit cursor and progress |

Schema 23 introduced local integer `event_key`/`charge_key`, never served IDs.
Event digests remain dedup identities; charge digests are retired. The predecessor
FK and `priced_charge_supersedes_same_measurement` require an existing
same-measurement predecessor even with deferred FKs. VACUUM and a full backup
baseline follow rebuild commit offline. `spend.charge.explain` serves role, unit,
generation and predecessor generation.

Triggers `usage_event_no_delete`, `measurement_no_update` and `priced_charge_no_update`
abort UPDATE and DELETE. Corrections append rows; see the [data model](docs/data-model.md).

## Hourly summaries

Migration 24 seeds history dirty without removing per-call facts. UTC-hour cells
key producer, provider, model, account, auth selection, basis, work class, project
and nullable currency through `summary_value`; undated is separate. Checked i128
sums persist as decimal text with presence counts. Exact head-value frequencies
for complete, dated, currency-known events preserve percentiles without adjustments.
Budget uncertainty keeps its separate no-head/any-unpriced predicate.

Event/header, adjustment and transition triggers mark hours in their transaction.
Measurements share event insertion; charge writers must bump the header first.
`priced_charge_matches_event_generation_and_basis` checks agreement, not transaction
identity; no child dirty write is needed.

`SummaryMaintenance` reduces fresh 5000-event read chunks, replacing an hour only
if its revision still matches in the ordered writer transaction; no total deltas.
Dirty, missing and timezone/plan seam hours read source facts in the report
snapshot; broad scopes retain the original path. Oversized publications stay live.
A persisted independent full-ledger audit requeues missed marks and logs progress,
restarting an hour after completion. Stop interrupts work; failures back off an hour.
Multiplier/recompute and per-event explanation keep their original paths.

## Write ordering

`SharedState` has one store, a writer fence and two connections. `ordered_write`
and `ordered_op_write` commit under the fence, refreshing the projection for
primary changes. Network/file inputs precede writes; statement imports require
regular files up to 64 MiB. A failed projection refresh leaves committed data and
stale health; a write panic rolls back and stops non-zero for supervisor restart.

Served reads use `with_read_op` on the mutex-guarded read-only WAL connection:
one snapshot without the ingest writer fence. Summary chunks and multiplier
recompute use this reader; pricing/reprice/basis planning uses `with_conn` on the
writer. The reader cannot write and needs no lease.

## Producers

- **BROCA store.** `drain_once` reads one page of `export_facts` from BROCA's
  `run-index.db` in its data directory (`mode=ro`) past the cursor
  (`<instance>:<epoch>`, `export_seq`); a new instance or epoch restarts at 0. It
  refuses a lineage regression and commits facts and cursor in one transaction. An
  absent store removes the row; any drain failure keeps the cursor, sets `pending_unknown`.
- **Harness files.** Each tick reads the local harness session stores through
  `astrocyte-readers`, one isolated drain per source key; see
  [harness readers](docs/harness-readers.md). Goose emits one total per session.
- **Declared Claude workers.** `claude-code-workers` reads the harness module's
  private worker roots declared under the shared data home, independently of the
  main Claude Code root. Events keep producer `claude-code` and request identity,
  with the declared `work_class`. A post-commit read acknowledgement lets the
  harness delete finished workers; see the [binding contract](docs/claude-workers-contract.md).
- **Push.** Peers call `spend.event.ingest`, `spend.correction.ingest` and
  `spend.chargeability.settle`; see [retry rules](docs/money-ingest.md).

Every producer writes `usage_event`, `measurement` and `chargeability_transition`.
`fact.rs` mints identity locally; `producer_ref` values stay verbatim as provenance.

BROCA, Claude Code, Codex, OpenCode and Pi set unreadable records aside as
`set_aside_record` evidence, committing evidence, good events and progress together.
Only new evidence warns; rereads refresh last-seen time. Coverage reports counts
without degrading health.

## Refresher loop

`start_refresher` runs `run_tick` on a dedicated OS thread, once per
`poll_interval_secs`. One tick:

1. Connect a pending fusiform catalog and find where its history begins: from
   `history_begins_at_ms` in `catalog.status`, or, when an older fusiform omits it,
   from a refused time-zero `catalog.get`. Until then, step 4 waits.
2. Drain OpenCode, Pi, Claude Code, Codex, Gemini CLI, Goose, Oh My Pi, Kimi CLI,
   declared Claude workers and BROCA, skipping each producer `harness_ingest` turns
   off. A skipped producer is not read and its `source_runtime` row is removed, as for
   an absent harness store.
3. Consume insula quota events when `qta_connection_file` is set. A stoppable bus
   thread binds the naming table's existing durable without provisioning topology.
   It fetches `events_get` bodies and verifies exact-byte digests; account commits
   or recorded snapshot obligations precede acks, and duplicates do not write again.
   First notices, new epochs, gaps and invalid/missing bodies owe `usage.get` on the
   next tick. Healthy readers backstop at the earlier of ten minutes or one cadence
   before the unchanged freshness horizon (normally 90 seconds, versus 30-second
   polling). Account commits reset freshness; empty pulls do not. Without a current
   credential, attachment or recent successful pull, every tick polls as before.
   Failed polls retain windows; capacity remains advisory.
4. Price queued events through `run_pricing_walk`. With no catalog configured,
   events are stored unpriced with reason `catalog_unconfigured`.
5. Spend a bounded budget reconciling `pricing_pending` against the full ledger, to
   catch any write path that failed to enqueue.

6. Rebuild/audit hourly summaries in a shared 1.5-second soft budget, after normal
   ingestion, pricing and queue reconciliation.

An unrequested refresher end or handler panic stops the module non-zero. Failed
handlers attempt a small correlated `internal` error reply. EOF, GOODBYE and SIGTERM
keep their clean-stop paths.

The ckbus route shares HELLO's single nonce handoff. Daemon validation of
`consumer_identity` stamps its principal; nonces stay out of diagnostics.
The refresher drains account writes between ticks (250 ms idle wait) and joins
the bus reader on its stop flag. Mode changes are logged, not added to health schemas.

After each tick, `astrocyte-core/src/multiplier.rs` attempts missing completed UTC
days among the last seven, oldest first; each covers seven days ending at 00:00Z.
It waits for source progress or two hours, disclosing stale sources in
`error_deflating`; absent/skipped sources do not block. It reads `window_sample`
and scoped models, calls fusiform outside transactions, then appends in its own
write. Failures retry without rows, logged once per day/reason.
`rate_fact_unrecognized` fails the day; `no_catalog_coverage` counts unpriced tokens.
Older days need operator recompute.

Ledger pricing and the multiplier share `parse_rate_key` in
`astrocyte-core/src/catalog_client.rs`: `rate.<role>` or
`rate.<role>.<selector>.<value>`. `above_context.<threshold>` tests prompt size;
`mode.<name>` requires a stated non-standard mode. Missing mode rates are unpriced,
not fallback rates. No event stores a mode yet; multiplier rows disclose
`mode_unrecorded_tokens` beside `base_rate_tier_untested_tokens`.
Both paths honor `reasoning_billed_as_output`; unlisted reasoning needs its own
rate. Ledger coverage and whole-event `rate_tiered` refusals precede selection;
priced charges record the selected `fact_key` in `catalog_entry`.

A unique index prevents a second original per provider/day. Observations wait until
the catalog's history boundary is known. `scoped_producer_not_ingested` refuses scopes
including excluded producers before calls or writes: their quota counts traffic
whose tokens ingestion omits. `spend.multiplier.recompute` uses current config and
catalog, superseding the head or creating an original; only missing originals wait
for sources. `spend.multiplier.report` serves unsuperseded heads by day. Unknown
providers and failed computations write nothing.

## Desired configuration

The config is a closed JSONC object with six required members:
`poll_interval_secs`, `qta_connection_file`, `fusiform_connection_file`,
`subscription_multiplier`, `harness_ingest`, `reasoning_billed_as_output`.
Unknown or missing members fail decoding at every level. `harness_ingest.exclude` names source keys from Producers (and
`broca`); unknown or repeated keys are refused, as is `enabled: false` with a non-empty
`exclude`. Before migration 15, stored generations have no multiplier declaration;
the daily stage skips them until a reload. Before migration 17, they have no
`harness_ingest` and restore as all on. The reasoning list is stored in the
generation JSON; older generations lift the nested multiplier list or use an empty
list. New desired documents reject the nested form. `hydrate_startup` reads the file once,
so every change takes effect at the next start. `ck-astrocyte --check-config <path>`
validates a file without opening the store or the network. If no generation was ever
committed, an invalid file at startup stops the module; otherwise it keeps the
committed generation and degrades health. See [configuration](docs/configuration.md).

## Served operations

`PUBLIC_OPERATION_NAMES` in `crates/astrocyte-module/src/ops.rs` is the closed
surface; dispatch and the manifest both use it.

| Operation | Kind |
|---|---|
| `budget.verdict` | read |
| `spend.report` | read |
| `spend.subscription.report` | read |
| `spend.workclass.report` | read |
| `spend.ledger.coverage` | read |
| `spend.charge.explain` | read |
| `spend.reconcile` | read, statement reconciliation |
| `spend.event.ingest` | write |
| `spend.correction.ingest` | write |
| `spend.chargeability.settle` | write |
| `spend.statement.import` | write |
| `spend.subscription.set` | write |
| `spend.subscription.close` | write |
| `spend.event.reprice` | write, operator repricing |
| `spend.basis.correct` | write, charge-basis correction |
| `spend.multiplier.report` | read, multiplier heads by day |
| `spend.multiplier.pace` | read, hourly usage by producer and work class at the latest head's rates |
| `spend.multiplier.recompute` | write, operator recompute of a multiplier day |

Astrocyte calls QTA `usage.get` and fusiform `catalog.status`, `catalog.get` and
`plan.prices`. The contract is the [module surface](docs/module-surface.md).

## Health

`PublicProjection::health` reads only the in-memory projection. Health degrades when
the configuration is degraded, a producer's last drain failed, or a source has no
success within four poll intervals. Capacity freshness is reported separately and
uses four times the capacity cadence.
