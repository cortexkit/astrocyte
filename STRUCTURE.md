# Codebase Structure

A Cargo workspace of three crates. `ARCHITECTURE.md` explains how the parts work
together; this file says where they live. To find a symbol, search the code.

## Repository

```text
crates/
  astrocyte-core/     ledger domain logic and the SQLite schema (no subc dependency)
  astrocyte-module/   the ck-astrocyte binary, peer clients and source mappings
  astrocyte-readers/  harness session-store parsers
docs/                 public contracts, data model and operator guides
scripts/              local and CI gates, train push tooling
.github/workflows/    CI (ci.yml): checks, lints and tests on Linux and Windows
```

Internal records live in the ignored `.cortexkit/` folder, which tracks only
`WORKER.md`, the build and test guide for workers.

## `crates/astrocyte-core/src`

- `store.rs`: `MIGRATIONS` (1 to 39), connection setup and `migrate_store`.
- `fact.rs`: call and measurement identity.
- `ingest.rs`: recording calls in one transaction, redelivery and quarantine,
  chargeability settlement, adjustments and charge-basis correction.
- `pricing.rs`: pricing one measurement, and resolve-then-commit repricing.
- `catalog_client.rs`: the typed fusiform catalog boundary and `parse_rate_key`.
- `media.rs`: maps colliculus usage records into calls without rounding.
- `attempt_usage.rs`: decodes shared attempt usage without inventing counts.
- `subscription.rs`: effective-dated ledger plans and coverage.
- `vault_plan.rs` keeps immutable tier history, seeds old config and reconciles
  vault-owned ledger plans.
- `multiplier.rs`: the subscription multiplier: config, valuation, the daily
  observation, recompute and report. `multiplier/clean.rs` finds clean
  account-hours; `multiplier/calibration.rs` holds the stability checks.
- `statement.rs`, `statement_import.rs`, `reconcile.rs`: vendor statements, CSV
  import and statement-to-ledger reconciliation.
- `report.rs`: the read model behind the spend reports, coverage and explain.
  `report/hourly.rs` builds, reads and audits the hourly summaries.
- `runs.rs`: session, run and flow cost reads, with their hourly totals.
- `cache_diagnostics.rs`: harness steps per session and their cache verdicts.
- `step_feed.rs`: the live step feed table, retention and paged read.
- `capacity.rs`: insula quota windows, readings and freshness.
- `verdict.rs`: `budget.verdict`.
- `money.rs`: signed nanodollar arithmetic with one rounding site.
- `canon.rs`: canonical JSON and SHA-256 hashing for identity.
- `test_support.rs`: shared test fixtures.

## `crates/astrocyte-module/src`

A library shared by three binaries: `main.rs` (`ck-astrocyte`) and two tools,
`bin/spend_drive.rs` (calls one operation over a connection file) and
`bin/corpus_drive.rs` (drives real exported facts through ingest).

- `main.rs`: startup, `--version`, `--check-config`, the manifest (also printed
  by `--manifest`), serving and route-level refusals.
- `ops.rs`: `PUBLIC_OPERATION_NAMES` and `dispatch`.
- `state.rs`: `SharedState`, the writer fence, the projection and health.
- `config.rs`: closed configuration decoding and `INGEST_SOURCES`.
- `refresher.rs`: the loop: full ticks, the fast lane, pricing, the queue check,
  summaries and the multiplier stage. It also holds `harness_drain`, the drain
  plumbing every file source shares.
- Harness sources, one per source key: `claude_code_source.rs`, `codex_source.rs`,
  `opencode_source.rs`, `opencode_v2_source.rs`, `pi_source.rs`,
  `gemini_source.rs`, `goose_source.rs`, `omp_source.rs`, `kimi_source.rs`.
  `jsonl_schedule.rs` holds each JSONL file's next read deadlines, such as the
  staggered daily full re-read.
- Declared folders: `declared_roots.rs` (roots files and path containment),
  `claude_workers_source.rs`, `claude_thalamus_source.rs` and
  `opencode_thalamus_source.rs`. `harness_paths.rs` resolves every harness path.
- `broca_source.rs`, `wire_map.rs`: BROCA's `run-index.db` reader and fact mapping.
- `colliculus_source.rs`: the colliculus media usage feed.
- `thalamus_payer_source.rs` mirrors payer facts and records row-less calls;
  `thalamus_payer_join.rs` holds the transcript payer wait queues.
- `fusiform_client.rs`, `qta_client.rs`, `peer_exchange.rs`: the fusiform and
  insula clients and their shared framing. `capacity_map.rs` maps quota answers.
- `quota_events.rs`, `quota_bus_client.rs`: the bus reader for insula's quota
  events, with polling failover, and its attested ckbus calls. `runner_steps.rs`
  admits live runner step events into the step feed.
- `vault_source.rs` reads vault metadata and watch notices outside recording
  transactions, then applies plan changes on full ticks.
- `recent_steps.rs`: the in-memory recent steps behind `usage.cache.steps`.
- `step_feed.rs`: serving the step feed, its subscribers and who may read it.
- `test_support.rs`: test fixtures, also built under the `test-support` feature.
  The crate's `build.rs` stamps build provenance into the binary.

## `crates/astrocyte-readers/src`

- `sessions/`: one parser per harness: `claudecode.rs`, `codex.rs` (with
  `codex_archive.rs` for compressed rollouts), `opencode.rs`, `opencode_v2.rs`,
  `opencode_schema.rs`, `pi.rs`, `gemini.rs`, `goose.rs`, `omp.rs`, `kimi.rs`.
- Shared reader parts: `sessions/jsonl_resume.rs` (resumable JSONL reads),
  `sessions/sqlite_read.rs` (read-only snapshot opens), `sessions/roles.rs`
  (token roles), `sessions/lenient.rs` (diagnostic-only fields),
  `sessions/utils.rs` and `sessions/mod.rs`.
- `provider_identity.rs`, `file_identity.rs`, `sqlite_uri.rs`, `read_io.rs`:
  provider names, opened-file identity, read-only SQLite URIs and read counters.

Most parsers are adapted from tokscale; see `THIRD-PARTY-NOTICES.md`.

## Tests and vectors

Unit tests sit beside the code under `#[cfg(test)]`, some in sibling files whose
names end in _tests. Integration tests are in each crate's `tests/`. Wire vectors
that apps decode are in `crates/astrocyte-module/test-vectors/`, regenerated by
tests; samples pinned from peers are in `tests/vectors/`.

## Scripts

- `scripts/gate.sh`: CI's checks before a push: local checks, then the Linux and
  Windows suites on the remote build servers; with --fast, only the local checks.
- `scripts/check_doc_references.py`: checks every path, identifier, table and
  operation named in the two overview docs, and their size ceiling.
- `scripts/fmt-check.sh`, `scripts/check-path-deps.py`,
  `scripts/check-cortexkit-layout.sh`, `scripts/check_train_preconditions.py`:
  formatting, no outside path dependencies, nothing internal tracked, CI shape.
- `scripts/train-push.sh`, `scripts/watch-ci.sh`, `scripts/lib/`: landing on
  master through CI.

## Docs

- Contracts: `docs/module-surface.md` (every operation), `docs/money-ingest.md`,
  `docs/claude-workers-contract.md`,
  `docs/thalamus-claude-code-sessions-contract.md`,
  `docs/thalamus-opencode-sessions-contract.md`.
- How it works: `docs/data-model.md`, `docs/harness-readers.md`,
  `docs/subscription-multiplier.md`.
- Operators: `docs/configuration.md`, `docs/operations.md`, and
  `docs/prod-config/astrocyte-v2.jsonc` as an example configuration.

## Where new code goes

- **Schema:** a new `Migration` at the end of `MIGRATIONS` in `store.rs`. Never
  edit an applied migration.
- **A served operation:** its logic in `astrocyte-core`, its name in
  `PUBLIC_OPERATION_NAMES` and `dispatch`, and its contract in
  `docs/module-surface.md`.
- **A harness:** the parser in `astrocyte-readers`, the source in
  `astrocyte-module` on `harness_drain`, and its key in `config::INGEST_SOURCES`.
- **A peer client:** in `astrocyte-module`, with every module it calls declared in
  the manifest.
