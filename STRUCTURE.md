# Codebase Structure

`ARCHITECTURE.md` covers runtime; this maps code.

## Repository map

Crates live in `crates/`, public docs in `docs/`, and gates in `scripts/`.
`mutations.toml` tests guards by breaking them; `.github/workflows/ci.yml` runs CI.

## `astrocyte-core`

Domain logic and storage.

- `store.rs`: `MIGRATIONS` (1 to 29), `initialize_connection` and `migrate_store`.
  Migration 23 rebuilds integer keys without charge digests, checking FKs before
  commit; startup restores enforcement. VACUUM is a separate offline step.
- `fact.rs`: event and measurement identity, minted locally.
- `ingest.rs`: transactional event recording, redelivery handling, chargeability
  settlement, adjustments and charge-basis correction.
- `pricing.rs`: per-measurement pricing and resolve-then-commit repricing.
- `catalog_client.rs`: typed boundary to the fusiform catalog, and the one rate-key
  grammar (`parse_rate_key`) that ledger pricing and the multiplier share.
- `subscription.rs`: effective-dated subscription plans and coverage resolution.
- `multiplier.rs`: the derived subscription multiplier: config, Δused%, list-price
  token valuation, the daily observation, operator recompute and report.
- `statement.rs`, `statement_import.rs`: vendor statements, CSV import, line matching.
- `reconcile.rs`: statement-line against ledger reconciliation.
- `report.rs`: the read model behind reports, coverage and explain; explain serves
  charge generations within a measurement, not local surrogate keys.
- `report/hourly.rs`: This module builds and audits hourly summaries and combines
  clean summaries with live base rows for reports.
- `verdict.rs`: the machine-scoped `budget.verdict` read.
- `broca_step.rs`: insert-once BROCA diagnostic facts and bounded retention.
- `cache_diagnostics.rs`: harness and BROCA steps per session and their cache verdicts.
- `step_feed.rs`: the live step feed: table, change detection, paged read.
- `capacity.rs`: QTA capacity windows, observations, runtime and freshness.
- `money.rs`: signed nanodollar arithmetic with one rounding site.
- `canon.rs`: internal exact-integer canonical JSON and SHA-256 hashing.
- `lib.rs`: crate root.

## `astrocyte-module`

Shared library; three binaries.

- `lib.rs`: shared modules and unit tests, compiled once.
- `main.rs`: startup, `--version`, `--check-config`, subc serving and the manifest.
- `ops.rs`: `PUBLIC_OPERATION_NAMES` and `dispatch` for the served operations.
- `state.rs`: `SharedState`, the writer fence, the public projection and health.
- `recent_steps.rs`: the in-memory recent steps behind `usage.cache.steps`.
- `step_feed.rs`: serving the live step feed and who may read it.
- `config.rs`: closed desired-configuration decoding and canonical identity.
- `refresher.rs`: the background loop (drains, capacity poll, pricing pass, pricing
  queue reconciliation, the daily multiplier stage and its operator recompute).
- `broca_source.rs`, `wire_map.rs`: BROCA `run-index.db` reader and fact mapping.
- `qta_client.rs`, `capacity_map.rs`: insula `usage.get`/`events_get` client and capacity mapping.
- `quota_events.rs`, `quota_bus_client.rs`: quota events and attested authentication;
  `quota_events_tests.rs` tests delivery, failover and shutdown.
- `fusiform_client.rs`: fusiform `catalog.get` and `plan.prices` client.
- `claude_code_source.rs`, `codex_source.rs`, `opencode_source.rs`, `opencode_v2_source.rs`,
  `pi_source.rs`, `gemini_source.rs`, `goose_source.rs`, `omp_source.rs`, `kimi_source.rs`:
  harness producers sharing `refresher::harness_drain`, each with its own
  mapping and cursor.
- `harness_paths.rs`: injectable harness paths.
- `claude_workers_source.rs`: declared Claude roots, containment and atomic post-commit
  read acknowledgements; shared Claude Code mapping.
- `build.rs`: stamps source and dependency provenance into the binary.
- `test_support.rs`: waits and fixtures; gated by tests or the `test-support`
  dev-dependency feature.

Two non-production tools in `src/bin`:

- `spend_drive`: management consumer calling one served operation over a connection file.
- `corpus_drive`: drives a producer's real exported facts through the ingest path.

## `astrocyte-readers`

Harness session-store parsers.

- `sessions/claudecode.rs`: Claude Code JSONL transcripts, folded to one call per
  request.
- `sessions/codex.rs`: Codex CLI rollouts, one call per advancing `token_count`.
- `sessions/gemini.rs`: Gemini CLI chat recordings, one call per message with `tokens`.
- `sessions/goose.rs`: the Goose SQLite store, one accumulated row per session.
- `sessions/omp.rs`: Oh My Pi JSONL sessions, one call per assistant message.
- `sessions/kimi.rs`: Kimi CLI wire logs, one call per `StatusUpdate`.
- `sessions/jsonl_resume.rs`: the resumable append-only JSONL read the JSONL readers
  share (Pi keeps its own).
- `sessions/roles.rs`: `DerivedRole`, a disjoint token role or why there is none.
- `sessions/opencode.rs`, `sessions/opencode_schema.rs`, `sessions/opencode_v2.rs`: OpenCode.
- `sessions/pi.rs`: Pi JSONL sessions.
- Most reader files are adapted from the tokscale project; see `THIRD-PARTY-NOTICES.md`.
- `sessions/mod.rs`, `sessions/utils.rs`: shared session types and helpers.
- `provider_identity.rs`: provider name normalization.
- `file_identity.rs`, `sqlite_uri.rs`: opened-file identity and read-only SQLite URIs.
- `tests/fixtures/pi_usage.jsonl`: a Pi fixture.

## Scripts

- `scripts/check_train_preconditions.py`: CI precondition check on `ci.yml`.
- `scripts/fmt-check.sh`: formatting gate.
- `scripts/train-push.sh`, `scripts/train-push.local.sh`, `scripts/watch-ci.sh`,
  `scripts/lib/operator-gh.sh`, `scripts/lib/workflow-gates.py`: operator push and
  CI-watch tooling.
- `scripts/check_doc_references.py`: validates paths, identifiers, tables and operations
  in both overview docs.
- `scripts/check-path-deps.py`: no path dependency outside the repo.
- `scripts/check-cortexkit-layout.sh`: nothing internal tracked under `.cortexkit/`.

## Docs

`docs/` is public only; internal records stay in the ignored `.cortexkit/`, which
tracks only `WORKER.md`, the build and test guide.

- Contracts: `docs/module-surface.md` (operations), `docs/money-ingest.md`,
  `docs/claude-workers-contract.md`, `docs/thalamus-opencode-sessions-contract.md`.
- Architecture: `docs/data-model.md`, `docs/harness-readers.md`.
- Operator guides: `docs/configuration.md`, `docs/operations.md`.
- `docs/prod-config/astrocyte-v2.jsonc`: an example desired configuration.

## Change placement

- Schema: add a new `Migration` in `store.rs`. Never edit one that has already been
  applied.
- A new served operation: add it to `PUBLIC_OPERATION_NAMES` and `dispatch` in
  `ops.rs`. Put its logic in `astrocyte-core`.
- A new pull producer: add its client and drain in `astrocyte-module`. A new harness:
  add its reader in `astrocyte-readers` and mapping in `astrocyte-module`. Add either
  source key to `config::INGEST_SOURCES` for `harness_ingest.exclude`; ingestion
  defaults on unless excluded.
- Tests sit beside the code under `#[cfg(test)]`.
