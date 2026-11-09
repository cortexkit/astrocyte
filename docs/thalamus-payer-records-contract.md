# THALAMUS payer records: who paid for each call (v1)

This is the binding contract between the thalamus-harness module, which sends
model requests for the Claude Code, OpenCode and Pi sessions it runs, and
astrocyte, which records their usage. Change it only by agreement of both
modules, and bump `schema_version` when an existing meaning changes.

## 1. Why it exists

Astrocyte reads the transcripts of THALAMUS-run sessions from their stores (see
the [Claude Code](thalamus-claude-code-sessions-contract.md) and
[OpenCode](thalamus-opencode-sessions-contract.md) session contracts). A
transcript says how many tokens a call used, but not which login paid, so those
calls are recorded with charge basis `unknown` and no account, and stay
unpriced.

The harness module knows the payer of every request it sends: its Claude Code
gateway picks the vault login before forwarding, and the auth plugins inside
OpenCode and Pi report each finished request to it. A few billable requests
never appear in any transcript at all, such as keep-warm requests and title
generation.

Live step events are not a billing record: they are published without replay,
so a missed event would be a missed call. Payer facts come through the durable
store below.

## 2. The payer store (written by the harness module)

Path: `<cortexkit data home>/thalamus-harness/payer/payer.db`, where the
cortexkit data home is resolved as in the session contracts. It is a SQLite
store in WAL mode, separate from the harness module's other stores. Exactly one
process writes it: the thalamus-harness module, which hosts both the Claude
Code gateway and the relay that receives the plugins' reports. The harness
module creates and changes it through versioned migrations, and never holds a
write transaction longer than one insert batch.

```sql
CREATE TABLE payer_meta (
  key   TEXT PRIMARY KEY,   -- 'schema_version' ('1') and 'incarnation'
  value TEXT NOT NULL
);
CREATE TABLE payer_session (
  harness         TEXT NOT NULL,   -- 'claude-code', 'opencode' or 'pi'
  session         TEXT NOT NULL,   -- the host's own session id
  owner_principal TEXT,            -- who the work is for, as in the roots files
  declared_at_ms  INTEGER NOT NULL,
  PRIMARY KEY (harness, session)
);
CREATE TABLE payer_record (
  seq                  INTEGER PRIMARY KEY AUTOINCREMENT,
  attempt_id           TEXT NOT NULL UNIQUE,
  harness              TEXT NOT NULL,
  session              TEXT NOT NULL,
  request_kind         TEXT NOT NULL,
  step_ref             TEXT,
  recorded_at_ms       INTEGER NOT NULL,
  provider             TEXT NOT NULL,
  model                TEXT NOT NULL,
  charge_basis         TEXT,
  auth_selection       TEXT NOT NULL,
  account_ref          TEXT,
  outcome              TEXT,
  provider_request_id  TEXT,
  provider_response_id TEXT,
  usage_json           TEXT
);
```

- **Incarnation.** `payer_meta.incarnation` is a random id written when the
  store is created and rewritten whenever the harness module detects that an
  older copy was restored. A restored copy could otherwise hand out `seq`
  numbers below astrocyte's read position, and astrocyte would skip real rows.
- **Sessions.** The harness module inserts the `payer_session` row before it
  forwards the session's first request, so astrocyte knows which transcript
  calls to wait for. Sessions that already exist when the harness module
  starts are inserted at start, before any of their requests go out.
  `harness` uses the step feed's host names (`claude-code`, `opencode`, `pi`),
  never a runner's wire name such as `opencode2`. `session` is the host's own
  session id, the one in the transcript astrocyte reads: OpenCode's `ses_…` id,
  Pi's session id from its file name, and Claude Code's session UUID.
  `owner_principal` is the session owner the daemon attested at admission.
- **One `payer_record` row per physical request that reached the provider.** A
  request held or refused before it was sent gets no row.

### Row members

- `attempt_id`: unique and never reused. It is the identity of the row.
- `request_kind`: `main` for a request whose usage astrocyte records from a
  transcript, or one of the row-less kinds whose usage it does not: `keep_warm`,
  `prime`, `resend_first_attempt`, `failed_before_row`, `title`,
  `compaction_hook` and `generate`. Any other value makes the row unreadable.
  - OpenCode's plugin kind `primary` replies with an assistant row that has
    tokens, so it is `main`, including requests from an agent named
    `compaction`.
  - OpenCode's own compaction (`compaction_hook`) and title generation write no
    row with usage; OpenCode adds their tokens only to the session's running
    totals. `generate` is stored nowhere.
  - **This list is tied to what astrocyte reads.** If astrocyte ever records a
    source that covers a row-less kind, that kind becomes `main` first, in a new
    schema version published before astrocyte ships the reader. Otherwise the
    same request would be counted twice.
- `step_ref`: only on a `main` row that the harness module matched to its
  transcript call. It is that call's own key: the entry's `requestId` (the
  `request-id` response header) for Claude Code, the message id for OpenCode,
  and the shared `pi_step_ref` key from `cortexkit-usage-steps` for Pi. A
  row-less row never carries one.
- `recorded_at_ms`: when the harness module wrote the row.
- `provider`, `model`: the provider and the model actually sent.
- `charge_basis`: `subscription_included` or `metered_api`. It is stated, never
  inferred from the login kind, because an API key can be a subscription plan
  key. NULL when unsure; any other value counts as unknown.
- `auth_selection`: the vault record id of the login used, as the vault names it.
- `account_ref`: the provider account id, when known.
- `outcome`, `provider_request_id`, `provider_response_id`: informational.
- `usage_json`: required on every row-less kind, NULL on `main` rows. It is one
  JSON object in BROCA's agreed attempt shape: absent counts are left out
  (absent never means 0), `cache_write_5m` and `cache_write_1h` appear when the
  vendor splits cache writes, `reasoning_included_in_output` is present
  whenever reasoning is, and a fallback mid-request carries `usage_by_model`
  (each `{model_used, usage}`) instead of top-level counts.

## 3. Reading (astrocyte)

Astrocyte opens the store read-only and reads rows past its saved
`(incarnation, seq)`, committing them together with its new position in its own
store. It closes the store after every read, so the harness module's deletes
never meet a held file. When the incarnation changes, astrocyte reads again
from the lowest `seq` and skips every `attempt_id` it already holds. A row
astrocyte can't read is set aside as evidence with its location and reason,
and reading continues.

- **A matched `main` row** gives its transcript call that charge basis,
  `auth_selection` and `account_ref`. A subscription call is then covered by
  its account's ledger plan, and a metered call is priced at list rates. The
  transcript stays the source of the token counts.
- **Waiting.** The payer row and the transcript row are written separately. A
  completed transcript call in a session listed in `payer_session` waits up to
  10 minutes, the readers' existing quiet window, for its payer row. Without
  one by then, it is recorded with basis unknown and no account. Coverage
  counts those calls, and health does not change.
- **A row-less row** is recorded as its own call: producer `thalamus-harness`,
  the session as lineage, `attempt_id` as the producer reference, measurements
  from `usage_json`, the payer from the row, and the session's
  `owner_principal` as work class.
- **An unmatched `main` row** records nothing and is counted. Its call is
  recorded from the transcript with basis unknown, so no call is counted twice,
  whatever the match rate is.
- **A recorded call is never rewritten.** A payer row that arrives after its
  call was recorded is ignored and counted. Two payer rows for one call with
  different payer facts are set aside, and the call is recorded with basis
  unknown. Calls recorded before this contract shipped keep basis unknown.

## 4. Deletion (the harness module)

The harness module deletes a `payer_record` row only when its session is done
and astrocyte's `watermark.get` (served under `log-read-watermark/v1`) is at or
past the row's `recorded_at_ms` plus 60 seconds. Astrocyte's watermark does not
pass a payer row until that row is committed in astrocyte's own store,
including a `main` row still waiting for its transcript call, which astrocyte
keeps itself. A `payer_session` row is deleted with the session's last
`payer_record` row. The harness module never deletes an unread row on a timer.
