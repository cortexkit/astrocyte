# Architecture

The source is authoritative; `STRUCTURE.md` maps it.

## System shape

Astrocyte (`ck-astrocyte`) records machine-scoped AI usage off the request hot path,
prices events in signed nanodollars against fusiform, and serves ledger, report and
reconciliation over subc. `budget.verdict` always returns `allow`, because cap
enforcement is not built.

The workspace has three crates:

- `astrocyte-core`: producer-neutral identity, ingest, pricing, subscriptions,
  statements, reconciliation, reports and the SQLite schema. It has no subc
  dependency.
- `astrocyte-module`: the `ck-astrocyte` binary. It holds the serving surface, desired
  configuration, the refresher loop, the subc clients and the producer mappings.
- `astrocyte-readers`: parsers for coding-harness session stores (Claude Code,
  Codex, OpenCode, Pi, Gemini CLI, Goose, Oh My Pi, Kimi CLI) and provider identity
  normalization.

## Durable state

`crates/astrocyte-core/src/store.rs` defines `MIGRATIONS` (versions 1 to 25).
`initialize_connection` enables and verifies foreign keys, probes SQLite JSON, triggers and
partial indexes, and sets a 5000 ms busy timeout. The read-only report connection
skips the table-creating probe and copies that timeout.

The [data model](docs/data-model.md#tables) describes every table in version 25 and its write rules.

Schema 23 uses local integer `event_key` and `charge_key` references, never served
IDs. Event digests remain the dedup/served identity; charge digests are retired.
The scalar predecessor FK and `priced_charge_supersedes_same_measurement` trigger
require an existing same-measurement predecessor, even with deferred FKs. Operators
run VACUUM after commit and start a full backup baseline. `spend.charge.explain`
identifies charges by role, unit and generation; `supersedes` serves the predecessor
generation.

Triggers `usage_event_no_delete`, `measurement_no_update` and `priced_charge_no_update`
abort UPDATE and DELETE. Corrections append rows; see the [data model](docs/data-model.md).

## Hourly summaries

Per-call rows remain authoritative, and hourly summaries are derived data that can
be rebuilt. Triggers mark the affected UTC hour or undated partition dirty in the
same transaction that records an event or changes its pricing, adjustments or
chargeability. The refresher rebuilds dirty hours from the base tables and
publishes a replacement only if the hour's revision still matches. Reports read
clean whole hours from summaries and read dirty, missing or timezone and
subscription seam hours from base rows in the same snapshot. Work-class
percentiles come from exact value frequencies rather than approximate buckets.
The audit compares summaries against base rows, requeues discrepancies and starts
its next pass an hour after the previous pass completes. Charge writers must
update the event header before inserting heads; the existing generation-and-basis
guard checks agreement, not transaction identity. The [data model](docs/data-model.md#hourly-summaries)
explains the grain, arithmetic, invalidation rules and maintenance budgets.

## Write ordering

`SharedState` holds one store, a writer fence and two connections. `ordered_write`
(refresher) and `ordered_op_write` (served operations) run one transaction under the
fence, then refresh the projection. Network and file I/O precede transactions;
statement import accepts regular files up to 64 MiB. A failed projection refresh
leaves the commit intact; health stays stale until a refresh succeeds. A write panic
rolls back and stops the module non-zero so the supervisor restarts it.

Served reads, including `budget.verdict`, use `with_read_op` on a mutex-guarded
read-only connection opened after migration. In WAL mode a report neither waits for
nor blocks ingest, and each read sees one ledger snapshot. Refresher reads and reads before
`spend.event.reprice` and `spend.basis.correct` use the writer via `with_conn`;
`spend.multiplier.recompute` reads on the reader before writing. The reader cannot
write, so it needs no lease.

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

An unrequested refresher end or handler panic stops the module non-zero. Failed
handlers attempt a small correlated `internal` error reply. EOF, GOODBYE and SIGTERM
keep their clean-stop paths.

The ckbus route shares HELLO's single nonce handoff. Daemon validation of
`consumer_identity` stamps its principal; nonces stay out of diagnostics.
The refresher drains account writes between ticks (250 ms idle wait) and joins
the bus reader on its stop flag. Mode changes are logged, not added to health schemas.

After each tick, `astrocyte-core/src/multiplier.rs` attempts missing observations
for the seven most recent completed UTC days, oldest first; each covers seven days
ending at 00:00Z. It waits for source reads past the boundary, or two hours, then
discloses stale sources in `error_deflating`. Absent/skipped sources do not block.
It reads `window_sample` and scoped models, calls fusiform `plan.prices` and
`catalog.get` outside transactions, and appends in its own write. Failures retry
without rows, logged once per day/reason. `rate_fact_unrecognized` fails the day;
`no_catalog_coverage` counts unpriced tokens. Older days need operator recompute.

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
