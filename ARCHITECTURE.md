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

`crates/astrocyte-core/src/store.rs` defines `MIGRATIONS` (versions 1 to 23).
`initialize_connection` enables and verifies foreign keys, probes SQLite JSON, triggers and
partial indexes, and sets a 5000 ms busy timeout. The read-only report connection
skips the table-creating probe and copies that timeout.

After version 23 these tables exist:

| Table | Holds |
|---|---|
| `usage_event` | one immutable usage event per producer reference and lineage |
| `measurement` | immutable per-event quantities by role and unit |
| `chargeability_transition` | append-only settlement of whether an event is chargeable |
| `quarantine_evidence` | quantities in a referring event that contradict the event it refers to |
| `rejected_delivery` | redeliveries whose payload differs from the stored event |
| `set_aside_record` | unreadable producer records keyed by source and locator, with reason and first/last observation times |
| `redelivery_log` | identical redeliveries from pushing producers |
| `adjustment` | signed money adjustments (write-off, restatement, repricing, refund, goodwill) |
| `priced_charge` | append-only priced charge chain per measurement, with origin |
| `pricing_pending` | events that still have a measurement with no current charge head |
| `pricing_selection_state` | plan generation and backfill-drained generation for coverage backfill |
| `account_subscription` | effective-dated subscription plans per provider account |
| `statement` | vendor statement identities |
| `statement_line` | immutable statement line content |
| `match_attempt` | append-only statement-line to ledger matches, with the amounts captured |
| `statement_import` | CSV import provenance and content idempotency |
| `desired_config` | the committed desired-configuration generation |
| `source_runtime` | per-producer cursor, lineage, last success and pending disclosure |
| `capacity_runtime` | capacity axis state: `disabled`, `configured_unavailable` or `active` |
| `capacity_window` | latest QTA quota windows per account |
| `capacity_observation` | latest capacity observation per identity |
| `capacity_provider_reading` | latest provider-level capacity readings |
| `window_sample` | append-only capacity window history and breakdown |
| `multiplier_observation` | append-only daily subscription multiplier: one original row per quota provider per UTC day, and operator recomputes that supersede it |

Schema 23 uses local integer `event_key` and `charge_key` references, never served
IDs. Event digests remain the dedup/served identity; charge digests are retired.
The scalar predecessor FK and `priced_charge_supersedes_same_measurement` trigger
require an existing same-measurement predecessor, even with deferred FKs. Operators
run VACUUM after commit and start a full backup baseline. `spend.charge.explain`
identifies charges by role, unit and generation; `supersedes` serves the predecessor
generation.

Triggers `usage_event_no_delete`, `measurement_no_update` and `priced_charge_no_update`
abort UPDATE and DELETE. Corrections append rows; see the [data model](docs/data-model.md).

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
3. Poll QTA `usage.get` when `qta_connection_file` is set. A failed poll records the
   attempt and keeps the last windows. Capacity is advisory and never blocks spend.
4. Price queued events through `run_pricing_walk`. With no catalog configured,
   events are stored unpriced with reason `catalog_unconfigured`.
5. Spend a bounded budget reconciling `pricing_pending` against the full ledger, to
   catch any write path that failed to enqueue.

An unrequested refresher end or handler panic stops the module non-zero. Failed
handlers attempt a small correlated `internal` error reply. EOF, GOODBYE and SIGTERM
keep their clean-stop paths.

After each tick, the multiplier stage (`astrocyte-core/src/multiplier.rs`) attempts
missing rows for each configured quota provider's seven most recent completed UTC
days, oldest first. Each row covers the seven days ending at its 00:00Z boundary.
It waits until every `source_runtime` row has a successful read at or after that
boundary; absent or skipped sources do not block. At two hours after the boundary it
records anyway, naming stale sources in `error_deflating` (reads-low text).
It reads quota Δused% from `window_sample` and scoped models on the reader connection,
calls fusiform `plan.prices` and `catalog.get` outside transactions, then reads and
prices tokens and appends in its own write. Failures write nothing and retry within
the look-back, logged once per day and reason. Unreadable catalog rates
(`rate_fact_unrecognized`) fail the day; absent models (`no_catalog_coverage`) count
as unpriced tokens on a written row. Older days are not tried automatically.

Ledger pricing and the multiplier read rate keys through one grammar,
`parse_rate_key` in `astrocyte-core/src/catalog_client.rs`: `rate.<role>` is the
unconditional rate and `rate.<role>.<selector>.<value>` a conditional one. Only a key
that fits neither shape, a role outside the five priced roles, or an `above_context`
threshold that is not a whole number is unreadable. `above_context.<threshold>` applies
when the request's prompt exceeds the threshold. `mode.<name>` applies when the
request's own record states that mode (for example `fast`); a request recorded as
`standard` or `default`, or with no recorded mode, prices at the unconditional rate,
and a request recorded in a mode the model has no rate for is unpriced. A selector
family no producer records never applies. No event stores a mode yet, so every stored
event prices at the unconditional rate; the multiplier counts those tokens as
`mode_unrecorded_tokens` on each model line whose model serves a mode rate, and the
row's reads-low text names that count beside `base_rate_tier_untested_tokens`.

Any row records its day; corrected days get no second automatic row. A partial unique
index refuses a second original per provider and day. While a configured catalog
is still discovering its history boundary, the day waits for it. A quota provider
whose scope names a producer the configuration does not ingest (excluded or ingestion
off) fails the day with `scoped_producer_not_ingested`: its quota still counts that
traffic, but its tokens would not.

`spend.multiplier.recompute` computes the same seven-day interval with current config
and catalog. Price calls precede writes. It supersedes the
day's head, or creates an original with null `supersedes`. Only missing
originals wait for source reads. A head is a row no
other row supersedes; `spend.multiplier.report` serves heads by day, not snapshot time.
Unknown providers and failed computations write nothing.
`scoped_producer_not_ingested` names excluded producers; every requested scope
is checked before calls or writes.

## Desired configuration

The config is a closed JSONC object with five required members:
`poll_interval_secs`, `qta_connection_file`, `fusiform_connection_file`,
`subscription_multiplier`, `harness_ingest`. Null means "not configured". An unknown or
missing member fails decoding, including inside `subscription_multiplier` and
`harness_ingest`. `harness_ingest.exclude` names source keys from Producers (and
`broca`); unknown or repeated keys are refused, as is `enabled: false` with a non-empty
`exclude`. A generation committed before migration 15 has no stored multiplier and
hydrates as "not recorded", so the daily stage skips it until the next start publishes
one. A generation committed before migration 17 has no stored `harness_ingest` and
hydrates as all on, which is how it ingested. `hydrate_startup` reads the file once,
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
