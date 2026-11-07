# Architecture

Astrocyte (`ck-astrocyte`) is the spend ledger for AI usage on one machine. It
records every model call and media job it can observe, prices each in signed
nanodollars against the fusiform catalog, and serves spend reports, run costs,
cache diagnostics, a live step feed and subscription multipliers over subc. It
reads usage after the fact, never on a call's path, and enforces nothing:
`budget.verdict` always answers `allow`. `STRUCTURE.md` maps the files; `docs/`
holds the contracts and details this page summarises.

## Rules the design keeps

- **Append-only.** Recorded calls, measurements and charges are never deleted or
  rewritten; a correction appends a new generation.
- **Absent is not zero.** An unreported count stays unknown, and an unpriced
  quantity stays unpriced with a reason.
- **Astrocyte mints identity.** `fact.rs` derives `event_id` from the producer,
  lineage (usually the session) and the producer's own reference, which is kept
  verbatim. Re-reading a store records nothing twice.
- **One pricer.** Producers send usage, never cost, so reports cannot disagree
  with the ledger.
- **One bad record never stops a source.** It is set aside as evidence with its
  location and reason, and reading continues.
- **No peer blocks startup or ends the process.** Work that needs a peer waits.
- **Per-tick cost follows new work.** Readers tail what changed; whole-history
  checks run daily.

## Terms

- **Call (event):** one model request or media job, a row in `usage_event`.
- **Measurement:** one quantity of one role on a call, such as input tokens.
- **Charge:** the price of one measurement. The head is the newest generation;
  repricing appends a head naming its predecessor.
- **Charge basis:** `metered_api` prices at list rates; `subscription_included` is
  covered when a ledger plan is in force; an unknown basis stays unpriced.
- **Source:** a place usage is read from, with its own key, cursor and health.
- **Work class:** who the work was for, verbatim from the producer.

## Crates

- `astrocyte-core`: identity, ingest, pricing, subscriptions, statements,
  reconciliation, reports, runs, cache diagnostics, the multiplier and the SQLite
  schema. It has no subc dependency.
- `astrocyte-module`: the `ck-astrocyte` binary: serving, configuration, the
  refresher loop, peer clients and the mapping from each source into the ledger.
- `astrocyte-readers`: parsers for harness session stores (Claude Code, Codex,
  OpenCode 1 and 2, Pi, Gemini CLI, Goose, Oh My Pi, Kimi CLI) and provider name
  normalisation. Most are adapted from tokscale.

## Durable state

One SQLite store in WAL mode, written by one process under a lease.
`crates/astrocyte-core/src/store.rs` holds `MIGRATIONS`, at schema version 37; an
older binary refuses a newer store. `initialize_connection` sets a 5000 ms busy
timeout and in-memory temporary storage, enables foreign keys and probes needed
SQLite features; commits use FULL sync. Triggers refuse deleting or rewriting the
append-only tables, even through `INSERT OR REPLACE`. Integer keys are never
served. The [data model](docs/data-model.md#tables) lists every table.

## Where usage comes from

- **Harness stores.** One reader per harness reads its session files or database
  read-only; see [harness readers](docs/harness-readers.md).
- **BROCA.** `drain_once` reads pages of `export_facts` from BROCA's `run-index.db`
  past a cursor, refuses a cursor that moves backwards, and commits facts and
  cursor together. BROCA plans to replace this with an acknowledged billing feed.
- **Declared folders.** THALAMUS lists its private sessions in roots files, read as
  `claude-code-workers`, `claude-code-thalamus` and `opencode-thalamus`. Each entry
  must resolve to its exact expected path. After commit astrocyte acknowledges
  reads (or serves a watermark through `watermark.get`), and only then may
  THALAMUS delete a folder. See the
  [workers](docs/claude-workers-contract.md),
  [Claude Code](docs/thalamus-claude-code-sessions-contract.md) and
  [OpenCode](docs/thalamus-opencode-sessions-contract.md) contracts.
- **Colliculus.** Media usage arrives through colliculus's feed (`usage.read`),
  each page committed with its cursor before `usage.ack`. A missing colliculus
  module leaves health untouched.
- **Push.** Peers call `spend.event.ingest`, `spend.correction.ingest` and
  `spend.chargeability.settle`; see [retry rules](docs/money-ingest.md).

Every source writes `usage_event`, `measurement` and `chargeability_transition`.
A call recorded again with identical content writes nothing; different content
is quarantined. Set-aside records go to `set_aside_record` in the same
transaction as the good calls and the cursor, and coverage counts them.

## Refresher loop

`start_refresher` runs one OS thread. A full tick, every `poll_interval_secs`:

1. Connect a configured fusiform catalog and learn its history boundary from
   `catalog.status`. Until then, pricing waits and calls stay queued.
2. Drain every source that `harness_ingest` allows. An excluded source is not read
   and loses its `source_runtime` row, like an absent one.
3. Read insula quota: commit queued quota events, and call `usage.get` when the
   event stream cannot be trusted or the freshness horizon is near (see below).
4. Price queued calls (`run_pricing_walk`), bounded per tick by time and count.
5. Spend up to two seconds on the daily check that every unpriced call is queued
   for pricing (`QUEUE_RECONCILE_INTERVAL`).

Then the loop rebuilds dirty summary hours for up to 1.5 seconds and runs the
multiplier stage. Between full ticks it applies quota events every 250 ms and,
every two seconds (`FAST_LANE_INTERVAL`), a fast lane: the harness readers except
Goose, the Claude worker folders and BROCA tail only what changed. Other sources
wait for full ticks. Fast cycles commit nothing when idle and never count as a
source's successful drain.

The bus reader thread holds one ckbus credential, opened with the launch secret
already given to the daemon. It reads insula's per-account quota notices from
astrocyte's durable, fetches each body through `events_get`, checks its digest
and acknowledges only after commit. A gap, new epoch, bad digest or missing body
asks for a full `usage.get`, which also runs at least every ten minutes, and on
every tick while events are not flowing. The same reader follows runners' live
`step.finished.v1` events into the step feed, never into the ledger.

## Pricing

Each measurement uses the catalog entry in force at the call.
`parse_rate_key` in `catalog_client.rs` is the one rate-key grammar,
shared with the multiplier. Token keys are `rate.<role>` per million tokens,
optionally conditioned on prompt size (`above_context`), `mode` or `ttl`. Unit
keys add a denominator after the role, such as `per_second` or `per_image`, and
may carry selectors such as image size. Unknown roles and units are refused.
Providers in `reasoning_billed_as_output` price reasoning at the output rate;
otherwise reasoning needs its own rate.

A charge records which catalog fact priced it, or why the measurement is unpriced
(for example `no_catalog_coverage` or `no_subscription_in_force`). Repricing
(`spend.event.reprice`) and basis correction (`spend.basis.correct`) append new
heads. The [data model](docs/data-model.md#money) covers units, rounding and the
charge chain.

## Hourly summaries

Reports read per-hour totals instead of scanning every call. The summaries are
derived; per-call rows stay authoritative. Triggers mark an hour dirty in the
transaction that records a call or changes its pricing, adjustments or
chargeability, and the loop rebuilds dirty hours from base rows, publishing only
if the hour has not changed since. A report combines clean hours with base rows
for dirty hours and for hours a timezone or plan boundary splits, in one
snapshot, so answers equal a full scan. `usage.runs.list` has its own hourly
table, kept the same way. A daily audit requeues any hour that differs from its
base rows. Details: [data model](docs/data-model.md#hourly-summaries).

## Subscription multiplier

The multiplier estimates how much list-price API value a subscription delivers per
unit of its quota. Each UTC day it writes one row per quota provider and plan for
the seven days before that midnight. It counts only clean account-hours: closely
bracketed by quota readings, unsaturated, with usable reset times and all calls
priced. `routing_eligible` and its reasons come from sample-size and stability
gates. Readings match by account id, or by vault credential for account-less
plans; plan models without prices are valued at their public API twin. Missed
days are retried for a week, and `spend.multiplier.recompute` appends a new row
for a day. The ledger is never changed. The rules and thresholds are in
[subscription multiplier](docs/subscription-multiplier.md).

## Cache diagnostics and the step feed

`usage.cache.sessions` and `usage.cache.steps` give each harness step a cache
verdict (cold start, stable, warning, bust, full bust or unknown, plus a
dropped-prompt flag) in session order, from the ledger and `step_detail`; recent
sessions come from memory (`recent_steps.rs`). The live step feed keeps a day of
normalised steps in `step_feed`, merging recorded steps with runner
announcements; a recorded row always wins. Subscribers get cursor notices and
page rows themselves. Only local apps and Callosum phones may read it.

## Serving and write ordering

The `subc-client-rs` serve helper owns the daemon connection and runs requests
concurrently. `SharedState` holds a fenced writer and one read-only connection.
`ordered_write` (refresher) and `ordered_op_write` (served writes) run one
transaction under the fence, then refresh the in-memory projection; network and
file I/O come first. Served reads use `with_read_op`, so a report never blocks
ingest.

Writes check no caller identity, so a flow's route may read but never write
(`flow_scopes_unsupported`). Only `thalamus-harness` may call `watermark.get`. A
write panic, stopped refresher or handler panic exits 1 so the supervisor
restarts the module; failed requests still get an `internal` reply. EOF and
GOODBYE exit 0. On Unix, SIGTERM stops cleanly and then re-raises.

## Configuration

The config is a closed JSONC object with six required members:
`poll_interval_secs`, `qta_connection_file`, `fusiform_connection_file`,
`subscription_multiplier`, `harness_ingest` and `reasoning_billed_as_output`. An
unknown or missing member fails decoding at every level, so the config file and
the binary must change together. `harness_ingest.exclude` takes keys from
`config::INGEST_SOURCES`. `hydrate_startup` reads the file once at startup, and
each accepted file becomes a stored generation. An invalid file stops the first
start; later it keeps the stored generation and degrades health.
`ck-astrocyte --check-config <path>` validates a file offline. See
[configuration](docs/configuration.md).

## Served operations

Dispatch and the manifest share `PUBLIC_OPERATION_NAMES` in
`crates/astrocyte-module/src/ops.rs`.

| Operation | Kind |
|---|---|
| `budget.verdict` | read |
| `spend.report` | read |
| `spend.subscription.report` | read |
| `spend.workclass.report` | read |
| `spend.ledger.coverage` | read |
| `spend.charge.explain` | read |
| `spend.reconcile` | read |
| `spend.event.ingest` | write |
| `spend.correction.ingest` | write |
| `spend.chargeability.settle` | write |
| `spend.statement.import` | write |
| `spend.subscription.set` | write |
| `spend.subscription.close` | write |
| `spend.event.reprice` | write, operator repricing |
| `spend.basis.correct` | write, charge-basis correction |
| `spend.multiplier.report` | read |
| `spend.multiplier.pace` | read |
| `spend.multiplier.recompute` | write, operator recompute |
| `usage.cache.sessions` | read |
| `usage.cache.steps` | read |
| `usage.runs.list` | read, session, run and flow totals |
| `usage.runs.detail` | read, one run's items |
| `usage.steps.subscribe` | read, watch |
| `usage.steps.read` | read, live step feed |
| `watermark.get` | read, THALAMUS read watermark |

Astrocyte calls fusiform (`catalog.status`, `catalog.get`, `plan.prices`), insula
(`usage.get`, `events_get`), colliculus (`usage.read`, `usage.ack`) and the ckbus
credential operations. The [module surface](docs/module-surface.md) is the
contract.

## Health

`PublicProjection::health` reads only the in-memory projection, never the store.
It degrades when the configuration is degraded, the last projection refresh
failed, a source's last drain failed, or any source has had no successful drain
within four poll intervals. Quota freshness
is reported beside it and does not change the status.
