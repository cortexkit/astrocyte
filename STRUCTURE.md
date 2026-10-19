# Codebase Structure

`ARCHITECTURE.md` covers runtime; this maps code.

## Repository map

Crates live in `crates/`, public docs in `docs/`, and gates in `scripts/`.
`mutations.toml` tests guards by breaking them; `.github/workflows/ci.yml` runs CI.

## `astrocyte-core`

Domain logic and storage.

- `store.rs`: `MIGRATIONS` (1 to 24), connection checks and migration runner.
  Version 23 rebuilds integer keys; version 24 seeds derived hourly summaries.
- `fact.rs`: event/measurement identity.
- `ingest.rs`: event recording, redelivery, settlement, adjustments and basis correction.
- `pricing.rs`: measurement pricing and two-phase repricing.
- `catalog_client.rs`: fusiform boundary and shared `parse_rate_key` grammar.
- `subscription.rs`: effective-dated plans and coverage.
- `multiplier.rs`: config, quota deltas, valuation, daily rows and recompute/report.
- `statement.rs`, `statement_import.rs`: statements, CSV and line matching.
- `reconcile.rs`: statement/ledger comparison.
- `report.rs`: reply types, original source reducers and per-event explanation.
- `report/hourly.rs`: exact cubes, revision-fenced rebuild/publication, read planner
  and independent audit; `report/hourly/copy_bench.rs` holds opt-in copy benchmarks.
- `verdict.rs`: machine-scoped verdict and original SQL oracle.
- `capacity.rs`: QTA windows, observations and freshness.
- `money.rs`: checked nanodollars and rounding.
- `canon.rs`: exact-integer canonical JSON and hashes.
- `lib.rs`: crate root.

## `astrocyte-module`

Shared library; three binaries.

- `lib.rs`: shared modules/tests.
- `main.rs`: startup, version/config checks, subc serving and manifest.
- `ops.rs`: closed `PUBLIC_OPERATION_NAMES` and dispatch.
- `state.rs`: store/reader, writer fence, projection and health.
- `config.rs`: closed configuration and canonical identity.
- `refresher.rs`: drains, capacity, pricing/queue reconciliation, `summary_tick`,
  daily multiplier and recompute.
- `broca_source.rs`, `wire_map.rs`: BROCA store reader and mapping.
- `qta_client.rs`, `capacity_map.rs`: insula reads and mapping.
- `quota_events.rs`, `quota_bus_client.rs`: quota events/authentication;
  `quota_events_tests.rs`: delivery, failover and shutdown.
- `fusiform_client.rs`: fusiform `catalog.get` and `plan.prices` client.
- `claude_code_source.rs`, `codex_source.rs`, `opencode_source.rs`, `pi_source.rs`,
  `gemini_source.rs`, `goose_source.rs`, `omp_source.rs`, `kimi_source.rs`: harness
  producers sharing `refresher::harness_drain`, with separate mapping/health/cursors.
- `harness_paths.rs`: injectable harness paths.
- `claude_workers_source.rs`: declared roots, containment and post-commit read acks.
- `build.rs`: binary provenance.
- `test_support.rs`: deadline waits/fixtures under tests or `test-support`.

Two non-production tools in `src/bin`:

- `spend_drive`: calls a served operation over a connection file.
- `corpus_drive`: feeds exported facts through real ingestion.

## `astrocyte-readers`

Harness session-store parsers.

- `sessions/claudecode.rs`: Claude Code JSONL, one call per request.
- `sessions/codex.rs`: Codex rollouts, one call per advancing token count.
- `sessions/gemini.rs`: Gemini messages with tokens.
- `sessions/goose.rs`: one SQLite total per session.
- `sessions/omp.rs`: Oh My Pi assistant messages.
- `sessions/kimi.rs`: Kimi status updates.
- `sessions/jsonl_resume.rs`: shared resumable JSONL reading (Pi keeps its own).
- `sessions/roles.rs`: `DerivedRole`, a disjoint token role or why there is none.
- `sessions/opencode.rs`, `sessions/opencode_schema.rs`: the OpenCode SQLite store.
- `sessions/pi.rs`: Pi JSONL sessions.
- Tokscale adaptations are credited in `THIRD-PARTY-NOTICES.md`.
- `sessions/mod.rs`, `sessions/utils.rs`: shared session types and helpers.
- `provider_identity.rs`: provider name normalization.
- `file_identity.rs`, `sqlite_uri.rs`: opened-file identity and read-only SQLite URIs.
- `tests/fixtures/pi_usage.jsonl`: a Pi fixture.

## Scripts

- `scripts/check_train_preconditions.py`: CI precondition check on `ci.yml`.
- `scripts/fmt-check.sh`: formatting gate.
- `scripts/train-push.sh`, `scripts/train-push.local.sh`, `scripts/watch-ci.sh`,
  `scripts/lib/operator-gh.sh`, `scripts/lib/workflow-gates.py`: operator push and
  CI tooling.
- `scripts/check_doc_references.py`: overview reference/schema/size checks.
- `scripts/check-path-deps.py`: no path dependency outside the repo.
- `scripts/check-cortexkit-layout.sh`: nothing internal tracked under `.cortexkit/`.

## Docs

`docs/` is public only; internal records stay in the ignored `.cortexkit/`, which
tracks only `WORKER.md`, the build and test guide.

- Contracts: `docs/module-surface.md` (operations), `docs/money-ingest.md`,
  `docs/claude-workers-contract.md` (worker roots and acknowledgements).
- Architecture: `docs/data-model.md`, `docs/harness-readers.md`.
- Operator guides: `docs/configuration.md`, `docs/operations.md`.
- `docs/prod-config/astrocyte-v2.jsonc`: an example desired configuration.

## Change placement

- Schema: append a `Migration` in `store.rs`; never edit applied migrations.
- A new served operation: add it to `PUBLIC_OPERATION_NAMES` and `dispatch` in
  `ops.rs`. Put its logic in `astrocyte-core`.
- A new pull producer: add its client and drain in `astrocyte-module`. A new harness:
  add its reader in `astrocyte-readers` and mapping in `astrocyte-module`. Add either
  source key to `config::INGEST_SOURCES` for `harness_ingest.exclude`; ingestion
  defaults on unless excluded.
- Tests sit beside the code under `#[cfg(test)]`.
