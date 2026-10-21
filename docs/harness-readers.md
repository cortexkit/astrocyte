# Harness readers

astrocyte records the AI usage of local coding harnesses by reading the
session stores they already write. It never asks a harness to report usage and never writes a
harness's store. The parsers are in `crates/astrocyte-readers`; each
harness's store location, ledger mapping and resume rules are in its
`*_source.rs` file in `crates/astrocyte-module/src`.

## 1. How a drain works

On every refresher tick each source is drained independently: a failure
degrades only that source's health. A source whose store is absent is not
ingested and has no `source_runtime` row. A source named in
`harness_ingest.exclude` is not read at all (see [configuration](configuration.md)).

Readers open SQLite stores read-only through a `mode=ro` URI
(`sqlite_readonly_uri`) inside one read snapshot, and read JSONL and JSON
files with plain reads. JSONL readers resume from a byte offset that always
ends on a newline; an unterminated tail is left for the next read. Before
trusting the offset they re-hash the consumed prefix with SHA-256: the last
64 KiB on ordinary content checks, the whole prefix on a periodic cadence
(`jsonl_resume.rs`; Pi keeps its own copy). A shrunk file, a changed inode or
a changed prefix makes the file be read again from the start. Events already
recorded then come back as exact redeliveries, because each event's identity
and content come from the file alone.

Pi, Claude Code (including declared workers), Codex, Oh My Pi, Kimi and Gemini
JSONL verify the whole consumed prefix at least every 24 hours. Each path has
a deterministic daily phase, so adding files does not move existing deadlines.
The first deadline is within 24 hours of a verified read, not a day plus an
offset. Appends never renew that deadline. Due files are checked whole, even
when one oversized file exceeds a soft byte budget. Cold starts and restarts
still read every file in full.

Every drain still discovers and stats every file. After one observed hour
without change, a complete file with no pending Claude calls may defer its
window check until five minutes after its last content check. An hour avoids
backing off ordinary pauses in active sessions. Changed size, modification
time or file identity wakes the file immediately; unknown metadata cannot
authorize deferral. Unchanged metadata never extends the five-minute bound.
Once a file change is detected, every-tick checking resumes, and a due
full-prefix check overrides deferral. Thus window rewrites and hidden appends
are detected on the next tick for active files, or within five minutes for
quiet files. Rewrites before the window are detected within 24 hours. Add
drain latency to these bounds. Changed payloads with existing event identities
reach quarantine rather than replacing ledger facts. Even an entirely deferred
drain commits and advances `source_runtime.last_success_ms`; stopped or rolled
back drains do not consume scheduled checks. Gemini JSON documents retain
their separate hourly verification policy. OpenCode and Goose are unchanged.

Codex `.jsonl.zst` archives are immutable snapshots, not appendable streams.
After a verified read, unchanged size, modification time and file identity skip
all decompression until the same staggered daily full-check deadline. A change
or replacement wakes the archive immediately. Unknown metadata never authorizes
deferral. Cold starts read every archive again.

Records a reader cannot use are counted, not guessed at. The BROCA, Claude
Code, Codex, OpenCode and Pi sources also store them as `set_aside_record`
rows (source key, locator, reason), committed together with the good events
and the resume position. `spend.ledger.coverage` reports them.

## 2. Sources

The source key is the `producer` stored on every event and the key used by
`harness_ingest.exclude`.

| Source key | Harness | Store |
|---|---|---|
| `claude-code` | Claude Code | `<CLAUDE_CONFIG_DIR or ~/.claude>/projects/**/*.jsonl` |
| `claude-code-workers` | Claude Code workers declared by thalamus-harness | `<root>/projects/**/*.jsonl` for each declared root |
| `codex` | Codex CLI | `<CODEX_HOME or ~/.codex>/sessions/` and `archived_sessions/`, `*.jsonl` and `*.jsonl.zst` |
| `opencode` | OpenCode | `OPENCODE_DB`, else `<XDG data home>/opencode/opencode.db`, table `message` |
| `pi` | Pi | `PI_CODING_AGENT_SESSION_DIR`, else `<PI_CODING_AGENT_DIR or ~/.pi/agent>/sessions/**/*.jsonl` |
| `gemini` | Gemini CLI | `<GEMINI_CLI_HOME or ~>/.gemini/tmp/`, `session-*.json(l)` and files under `chats/` |
| `goose` | Goose | `GOOSE_PATH_ROOT/data/`, else the platform data directory, `sessions/sessions.db`, table `sessions` |
| `omp` | Oh My Pi | `~/.omp/agent/sessions/**/*.jsonl` |
| `kimi` | Kimi CLI | `<KIMI_SHARE_DIR or ~/.kimi>/sessions/<group>/<session>/wire.jsonl` |
| `broca` | broca | `<data home>/cortexkit/broca/run-index.db`, table `export_facts` |

Each source resolves its store path by these rules, implemented in
`crates/astrocyte-module/src/harness_paths.rs`:

- `~` is `HOME`, or `USERPROFILE` on Windows. The XDG data home is
  `XDG_DATA_HOME`, else `~/.local/share`, on every OS, as OpenCode itself
  resolves it.
- An override variable that is set but empty counts as unset.
- A relative override is refused, never joined to the working directory, and
  so is a home that cannot be resolved. The source's drain then fails with the
  reason and its health is degraded. Tests:
  `empty_overrides_fall_back_and_missing_homes_refuse`,
  `opencode_relative_xdg_refuses_instead_of_falling_back`.
- Goose's platform data directory is `<XDG data home>/goose` on Unix and
  `%APPDATA%\Block\goose\data` on Windows (test: `goose_windows_default`).
- Oh My Pi has no override. It does not read `PI_CODING_AGENT_DIR`, which
  belongs to Pi, so the two sources never read one tree.
- The Claude worker roots come from
  `<data home>/cortexkit/thalamus-harness/claude-workers/roots.json`. The
  binding rules are the [Claude workers contract](claude-workers-contract.md).
- broca's store is read only at the default data-home path. A
  `BROCA_STATE_ROOT` elsewhere puts it where astrocyte does not look, and it
  reads as absent.

## 3. What one event is

| Source key | One event | `producer_ref` | `producer_lineage` |
|---|---|---|---|
| `claude-code`, `claude-code-workers` | One API request: the records sharing a `requestId`, folded at the per-field maximum | `requestId` | none |
| `codex` | One advancing `token_count` record (`last_token_usage`) | `<turn id>:<n>` for the n-th response of the turn, else `<thread id>:ordinal:<n>` | `session_meta.session_id` |
| `codex` | One otherwise unconfirmed `token_usage_record` | `<thread id>:request:response:<response id>`, else `<thread id>:request:ordinal:<n>` | `session_meta.session_id` |
| `opencode` | One assistant `message` row whose tokens are final (`time.completed` or `finish` set) | `message.id` | none |
| `pi` | One assistant message with usage | `<namespace>:response:<responseId>`, else a composite of entry id, timestamp, provider, model and counts | none |
| `pi` | One compaction with usage, or `usage` entry of kind `cache_warm` | `pi:operation:<entry id>` | none |
| `gemini` | One message with `tokens` | message id | `sessionId` |
| `goose` | One session's accumulated usage, not one call | `sessions.id` | `sessions.id` |
| `omp` | One assistant message with usage | entry `id` | session header `id` |
| `kimi` | One `StatusUpdate` record | `payload.message_id` | session directory name |
| `broca` | One `export_facts` row | `fact_id` | none |

Rules that affect what is recorded:

- A Claude Code response is written as one record per content block, each
  carrying the whole response's usage. A later copy with a lower counter
  refuses the call instead of lowering it. A call without `stop_reason` is
  recorded once its file has not grown for ten minutes
  (`CLAUDE_CODE_QUIET_WINDOW_MS`), with a null `terminal_status`. A call
  already recorded by the retired `thalamus` gateway producer, matched on
  `vendor_request_id`, is skipped. Tests:
  `per_block_copies_of_one_call_become_one_event_at_the_per_field_max`,
  `an_interrupted_call_is_ingested_once_after_the_quiet_window_with_null_status`,
  `a_call_already_recorded_by_thalamus_is_skipped`.
- A Codex subagent fork replays its parent's turns; replayed turns are
  skipped, and a moved rollout re-reads as exact redeliveries.
- A Codex archive uses the same parser and event identities as its plain twin;
  no archive suffix enters the identity. Records with the same producer, session
  lineage and producer reference but different content reach quarantine instead
  of replacing the first observation. Decoding is bounded to 1 GiB in total, 16 MiB per record and a
  64 MiB decoder window. An oversized record is discarded with set-aside evidence,
  and later records are still read. Corruption, truncation, checksum mismatch,
  trailing junk or an exceeded decoded-size limit fail the whole drain without
  committing partial usage or advancing success. Activity without supported usage
  produces a visible set-aside record; its token quantities remain unknown.
- Codex request records are matched to native token counts by reported input and
  output, regardless of which record appears first. A terminal event, new turn or
  subsequent request settles an unconfirmed request under its supplementary key.
  A compaction record marks the pending request; a later token count confirms it
  or settles it separately. EOF alone leaves it pending across drains, so appends
  and full replays agree. Late confirmations of settled requests are not counted
  again. These supplementary events never allocate native token-count positions.
- Pi operations use only their own model and provider, including absence. They
  never inherit the preceding assistant's model. Their reasoning remains nested
  in output, and their reported cost does not establish a charge basis. Existing
  assistant timestamps and fallback identities are unchanged; request-start
  timestamps are not adopted.
- A Goose session is recorded once its `updated_at` is 24 hours old
  (`GOOSE_QUIET_WINDOW_MS`). A session that grows after that is re-emitted
  and quarantined as a redelivery with a different payload; it is never
  summed into a second event. Tests:
  `a_session_is_one_event_once_its_quiet_window_has_passed`,
  `a_session_that_grows_after_being_recorded_is_quarantined_not_re_summed`.
- Declared Claude worker events keep producer `claude-code` and the same
  identity, and carry the declared `work_class`. Test:
  `worker_events_stamp_declared_class_without_changing_identity`.
- A timestamp that is absent or does not parse leaves the event undated. A
  file's modification time is never used instead.

## 4. Token roles

The ledger stores five disjoint token roles: `input` (uncached input),
`cache_read`, `cache_write`, `output` (visible output) and `reasoning`. A
harness that reports a role gross, with another role inside it, gives the
disjoint part only by subtraction. `DerivedRole` in `sessions/roles.rs` keeps
the outcomes apart:

| Outcome | Stored as |
|---|---|
| `Measured` | A measurement with the quantity. |
| `Absent` | No measurement: the harness did not report the counter. |
| `Underivable` | A measurement with no quantity and `source` ending `:underivable`: a counter needed for the subtraction is missing, or the harness's convention was never measured. |
| `Conflicted` | A measurement with no quantity and `source` ending `:conflicted`: the subtraction would go negative. Never clamped. |

A measurement without a quantity never prices. Each measurement's `source`
names the harness field it came from, and `kind` is `billed`. Test:
`difference_keeps_absent_unknown_and_impossible_apart`.

| Source key | `input` | `cache_read` | `cache_write` | `output` | `reasoning` |
|---|---|---|---|---|---|
| `claude-code` | `input_tokens` | `cache_read_input_tokens` | 5-minute and 1-hour writes as units `token_cache_5m` and `token_cache_1h`; unsplit `cache_creation_input_tokens` as `token` | `output_tokens` | not reported |
| `codex` | `input_tokens - cached_input_tokens`; underivable beside a non-zero cache write | `cached_input_tokens` | `cache_write_input_tokens` | `output_tokens - reasoning_output_tokens` | `reasoning_output_tokens` |
| `opencode` | `input` | `cacheRead` | `cacheWrite` | `output` (excludes reasoning) | `reasoning` |
| `pi` | `input` | `cacheRead` | `cacheWrite` | `output - reasoning`; a record with reasoning above output is set aside | `reasoning` |
| `gemini` | `input - cached`; underivable beside non-zero tool tokens or when the parts do not sum to `total` | `cached` | not reported | `output`; underivable when the parts do not sum | `thoughts` |
| `goose` | `accumulated_input_tokens`; underivable beside a non-zero cache counter | `accumulated_cache_read_tokens` | `accumulated_cache_write_tokens` | `accumulated_output_tokens` | not reported |
| `omp` | `input`; underivable beside a non-zero `cacheWrite` the total does not place | `cacheRead` | `cacheWrite` | `output - reasoningTokens`, or gross `output` when `reasoningTokens` is absent | `reasoningTokens` |
| `kimi` | `input_other`; underivable beside a non-zero `input_cache_creation` | `input_cache_read` | `input_cache_creation` | `output`, including reasoning | not reported |

Tests: `disjoint_roles_subtract_cached_input_and_reasoning_output`,
`a_non_zero_cache_write_leaves_input_underivable`,
`five_minute_and_one_hour_cache_writes_are_separate_measurements`,
`reasoning_measurement_is_stored_as_given_not_subtracted`,
`cached_input_is_subtracted_and_thoughts_kept_apart`,
`reasoning_is_subtracted_from_output_and_cache_read_kept_apart`,
`a_status_update_is_one_call_in_disjoint_roles`.

## 5. Provider, model and charge basis

Every harness event is recorded with charge basis `unknown`: no transcript
says whether the call was paid per token or covered by a subscription. Such
events stay unpriced with reason `charge_basis_unknown` until an operator
corrects the basis (see [operations](operations.md)). broca states a basis on
each fact. Test: `pi_event_is_unpriced_because_charge_basis_is_unknown`.

`model` is the harness's model name verbatim. `provider` is the harness's own
field where it has one (Codex `model_provider`, OpenCode `providerID`, Goose
`provider_name`, Oh My Pi and Pi `provider`). Claude Code and Gemini CLI
record none, so it is inferred from the model name by
`inferred_provider_from_model`; Pi assistant messages do the same when the record
has no provider. This includes Codestral and Devstral in the Mistral family.
Pi operations and Kimi CLI keep missing model and provider fields absent.
