STAND-IN: these rows come from a test stand-in, not from a real company gateway or a real model.

# Claude Code payer rows through a stand-in company gateway credential

Four `payer_record` rows (plus `payer_meta` and `payer_session`) from one short Claude
Code session whose model requests went out through a **company LLM gateway credential**,
written by thalamus' own code end to end: the real Claude Code 2.1.318 CLI, the module's
loopback gateway (`crates/thalamus-harness-module/src/claude_code/gateway/`), the gateway's
`billing` report (`gateway/payer.rs`), the runner's payer queue
(`runner/store/payer_queue.rs`, `take_billing`) and the payer writer (`payer.rs`,
`payer/store.rs`). No row, count or id was edited by hand.

Files:

- `payer.db` — the payer store, copied out of the live store by `VACUUM INTO` from a
  read-only connection once every forwarded request had its row. Rollback-journal mode
  (header bytes 18-19 = `01 01`), no `-wal`/`-shm`, `PRAGMA integrity_check` = `ok`. It also
  holds the store's bookkeeping tables (`cortexkit_*`, `high_water_*`, `sqlite_sequence`).
- `rows.json` — `payer_meta`, `payer_session` and every `payer_record` row of `payer.db`,
  every column by name, read from `payer.db` with Python's `sqlite3` module
  (`SELECT * ... ORDER BY seq`). NULL is `null`.
- `SHA256SUMS` — over `payer.db`, `rows.json` and this file.

## Where they come from

- Commit: `5aa2f9e4a921703f65088973b003e0a3b28cc8a5` (thalamus), plus one recording-aid
  test that was never committed: `record_company_gateway_payer_rows`, added beside
  `real_claude_code_uses_a_per_credential_company_gateway_and_metered_payer_row` in
  `crates/thalamus-harness-module/src/runner/tests_claude_code_real.rs` and built on it
  (same stand-in vault, same company-gateway upstream, same routing answer; it adds the
  tool-call turn, the 401 turn, `request-id` headers on the stand-in's answers, and the
  copy to disk).
- Permanent coverage of this path, in the repository:
  `runner::tests_claude_code_real::real_claude_code_uses_a_per_credential_company_gateway_and_metered_payer_row`
  (real CLI, one answered request, asserts provider/model/charge_basis/auth_selection of
  the row), and the gateway-level tests in `claude_code/gateway/upstream_tests.rs`
  (`company_gateways_receive_exact_paths_headers_and_rendered_body`,
  `gateway_refusals_are_typed_and_other_errors_pass_through_unchanged`,
  `gateway_credential_with_a_plan_keeps_the_plan_charge_basis`).
- Command, on the Linux build server (x86_64, with network for the fetch):

  ```
  bash probes/claude-code-linux/fetch.sh /tmp/cc
  THALAMUS_CLAUDE_CODE_BIN=/tmp/cc/claude RECORD_PAYER_OUT=$PWD/target/scratch/rec \
    cargo nextest run -p thalamus-harness-module --features test-support --run-ignored only \
    --no-capture -E 'test(record_company_gateway_payer_rows)'
  ```

  Result: 1 passed. The stand-in company gateway received exactly 4 requests, and the
  store holds exactly 4 rows.

## The stand-in setup

- Vault: the gateway tests' fake vault (`claude_code/gateway/tests.rs`, `FakeVault`). The
  chosen credential is `oauth:anthropic:two`, account `acct-2`, token `token-two` (a fixed
  fake string; it appears in no row).
- That credential's upstream is a company gateway, installed through the test-only seam
  `Vault::with_test_upstreams`: base URL `http://127.0.0.1:<port>/company` (a loopback
  stand-in server in the test process), auth header `x-portkey-api-key` with the raw
  token, fixed header `x-portkey-provider: @anthropic-prod`. So the requests went to
  `/company/v1/messages?beta=true`.
- Routing: a stand-in router answering every `route.account_for` with
  `{"account_id":"acct-2","credential_id":"oauth:anthropic:two","method":"gateway"}`.
- Model asked for: `claude-haiku-5-5` (a test name, not a real model id).
- The stand-in's answers are Anthropic-style event streams carrying a `request-id`
  response header `req_standin_gw_<n>` and message id `msg_standin_gw_<n>`.

## The rows

| seq | request | request_kind | outcome | step_ref / provider_request_id | provider_response_id | usage_json |
|---|---|---|---|---|---|---|
| 1 | turn 1 ("say something"): ordinary answered request, text answer, `end_turn` | `main` | `completed` | `req_standin_gw_1` | `msg_standin_gw_1` | NULL |
| 2 | turn 2 ("call echo please"), first request: the answer calls the plan tool `echo` (`stop_reason: tool_use`) | `main` | `completed` | `req_standin_gw_2` | `msg_standin_gw_2` | NULL |
| 3 | turn 2, second request: sent by Claude Code with the tool's result; text answer, `end_turn` | `main` | `completed` | `req_standin_gw_3` | `msg_standin_gw_3` | NULL |
| 4 | turn 3 ("once more"): the stand-in company gateway answered **401** `authentication_error` (with `request-id: req_standin_gw_4`); the turn ended in error `auth_required` | `failed_before_row` | `failed` | provider_request_id `req_standin_gw_4`, step_ref NULL | NULL | `{"usage":{}}` |

All four rows share: `harness` = `claude-code`, `session` =
`c8fc54c0-929c-46ed-a78a-d56360936a61` (Claude Code's own session id), `provider` =
`anthropic`, `model` = `claude-haiku-5-5`, `charge_basis` = `metered_api`,
`auth_selection` = `oauth:anthropic:two`, `account_ref` = `acct-2`.

On the 401 the gateway sent the request once: for a gateway credential it neither retries
on a renewed token nor reports the refusal to the vault; it tells routing
(`credential_refused`) and passes the 401 to Claude Code unchanged
(`gateway/mod.rs` around lines 892-958).

## Columns that matter for pricing

- `charge_basis` — `metered_api` on every row. The gateway sets it because the chosen
  credential's upstream is a company gateway and the vault record names no plan tier
  (`gateway/payer.rs` lines 67-75). A gateway credential whose vault record has a plan tier
  would get `subscription_included` instead (test
  `gateway_credential_with_a_plan_keeps_the_plan_charge_basis`). The 401 row is
  `metered_api` too, though nothing was served: its usage is unknown, not zero.
- Auth selection / method — `auth_selection` is the vault credential id the request went
  out on, `account_ref` its account. **There is no auth-method column**: routing's
  `"method":"gateway"` is not stored in the row, so a reader can't tell a gateway
  credential from this row alone except through `charge_basis` and the credential id.
- `provider` / `model` — `anthropic` and the model Claude Code asked for. The gateway
  always records the catalog provider (`gateway/payer.rs` line 23, and line 72: "Pricing
  uses the catalog provider/model, not the company's gateway id"); the per-gateway provider id
  (`anthropic-gw-<slug>`) and the gateway's base URL appear nowhere in the row.
- `usage_json` — NULL on the `main` rows by design: a Claude Code main row's usage is the
  transcript's, found through `step_ref` = the `requestId` Claude Code stores on that
  reply's transcript entry (`payer/report.rs`, `Row::main`). The stand-in's answers
  reported `input_tokens` 1200, `cache_read_input_tokens` 900,
  `cache_creation_input_tokens` 100 and `output_tokens` 42; those counts are only in Claude
  Code's transcript, not in this set. The 401 row carries `{"usage":{}}`: usage unknown.
- Request id — `provider_request_id` is the upstream's `request-id` response header;
  `step_ref` is the same value on a stored reply (`main`), NULL on a request that left no
  transcript entry (`failed_before_row`). `provider_response_id` is the `message.id` from
  the stream's `message_start`.
- `attempt_id` — a random UUID the gateway gives each forwarded request.
- `recorded_at_ms` / `declared_at_ms` — the build server's wall clock when the runner
  wrote the row / declared the session (2026-11-11T17:17:29-30Z on that machine).

## What a real company-gateway row will differ in

- `auth_selection`: the stand-in reuses a fake vault id shaped like an OAuth
  subscription (`oauth:anthropic:two`); a real gateway credential has its own vault id.
  Today the gateway's account roster only admits active Anthropic OAuth subscription
  records (`gateway/credential.rs` lines 282-297) and a company-gateway upstream can only
  be attached through the test-support seam (`credential.rs` lines 146-152); a shipped
  build cannot yet send through a real gateway credential at all.
- `account_ref`, `session`, `attempt_id`, `incarnation`, the times: real values.
- `model`: a real model id (whatever Claude Code asks for).
- `provider_request_id` / `step_ref` / `provider_response_id`: real ids, **if** the
  company gateway passes Anthropic's `request-id` header and `message.id` through. This
  stand-in sends both; if a real gateway drops `request-id`, both `provider_request_id`
  and `step_ref` are NULL and the row can't be tied to its transcript entry.
- The slug: none appears in the row (see `provider` above), so no real slug will either.
- Token counts: in the transcript, not here (see `usage_json`).
- `payer_session.owner_principal` is `prefrontal-core`, the test binding's owner; a real
  session names its real owner module.
