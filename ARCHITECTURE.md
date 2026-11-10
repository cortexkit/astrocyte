# Architecture

Astrocyte (`ck-astrocyte`) records local AI usage and prices it in nanodollars
against fusiform. It serves spend and usage diagnostics over subc.
`budget.verdict` always answers `allow`.
`STRUCTURE.md` maps the files; `docs/` holds the contracts.

## Rules the design keeps

- **Append-only.** Recorded calls, measurements and charges are never deleted or
  rewritten; a correction appends a new generation.
- **Absent is not zero.** An unreported count stays unknown, and an unpriced
  quantity stays unpriced with a reason.
- **Stable identity.** `fact.rs` hashes the producer, lineage and verbatim
  producer reference. Re-reading a store records nothing twice.
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

The leased SQLite store uses WAL, FULL synchronization and foreign keys, with a
5000 ms busy timeout. `store.rs` owns `MIGRATIONS`, now at version 40; an older
binary refuses a newer store. Immutable-table triggers reject rewrites even
through `INSERT OR REPLACE`. Integer keys are never served. The
[data model](docs/data-model.md#tables) describes the tables.

## Where usage comes from

- **Harness stores.** One reader per harness reads its session files or database
  read-only; see [harness readers](docs/harness-readers.md).
- **BROCA.** `drain_once` reads pages of `export_facts` from BROCA's `run-index.db`
  past a cursor, refuses a cursor that moves backwards, and commits facts and
  cursor together. BROCA plans to replace this with an acknowledged billing feed.
- **Declared folders.** THALAMUS lists its private sessions in roots files, read as
  `claude-code-thalamus` and `opencode-thalamus`. Each entry
  must resolve to its exact expected path. After commit astrocyte acknowledges
  reads (or serves a watermark through `watermark.get`), and only then may
  THALAMUS delete a folder. See the
  [Claude Code](docs/thalamus-claude-code-sessions-contract.md) and
  [OpenCode](docs/thalamus-opencode-sessions-contract.md) contracts.
- **THALAMUS payers.** `thalamus-payer` mirrors the harness's payer store before
  transcripts read. It attributes their calls, records requests absent from
  transcripts and joins the session watermark. See the
  [payer contract](docs/thalamus-payer-records-contract.md).
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
every two seconds (`FAST_LANE_INTERVAL`), a fast lane: BROCA and the harness
readers except Goose tail only what changed. Other sources
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

Reports combine clean hourly summaries with base rows for dirty or split hours
in one snapshot, so answers equal a full scan. Triggers mark changed hours
dirty; rebuilds publish only unchanged hours. Per-call rows stay authoritative.
Run totals use the same method. A daily audit requeues differing hours.
Details: [data model](docs/data-model.md#hourly-summaries).

## Subscription multiplier

The multiplier records clean account-hour API list value per quota point, once
per UTC day, quota provider and plan over a trailing week. It joins day-end vault
tier history by account or credential ID, seeded once from old config; scope and
windows remain configured. `routing_eligible` carries sample-size and stability
results. It retries missing days for a week and appends recomputations without
changing the ledger. The [subscription multiplier](docs/subscription-multiplier.md)
explains the thresholds and public API-twin valuation.

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

Direct ledger writes have a daemon confirmation gate, off until release; enabling
it refuses writes wherever the deployed daemon cannot confirm the person.
A flow's route may call only the manifest's
`flow_operations`; all other operations are refused `flow_scopes_unsupported`
before dispatch. Flows cannot read the step feed or `watermark.get`, and allowed
run reads retain their principal checks. Only `thalamus-harness` may call
`watermark.get`. A write panic, stopped refresher or handler panic exits 1 so
the supervisor restarts the module; failed requests still get an `internal`
reply. EOF and GOODBYE exit 0. On Unix, SIGTERM stops cleanly and then re-raises.

## Configuration

The closed JSONC config has six required members, detailed in
[configuration](docs/configuration.md). Config and binary deploy together;
unknown or missing members fail decoding. Accepted files become stored
generations. An invalid first file refuses startup; later failures retain the
committed generation and degrade health. `ck-astrocyte --check-config <path>`
validates a file offline. Live multiplier account maps are removed; old stored
maps seed plan history before hydration discards their null-override declarations.

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
credential operations. Claustrum supplies plan tiers through attested list-only
reads and a held watch, never secret reads. Full ticks apply changes without
restarting, protect hand-set plans and report missing prices or vault failures.
The [module surface](docs/module-surface.md) is the
contract.

## Health

`PublicProjection::health` reads only the in-memory projection, never the store.
It degrades when the configuration is degraded, the last projection refresh
failed, a source's last drain failed, or any source has had no successful drain
within four poll intervals. Quota freshness
is reported beside it and does not change the status.
