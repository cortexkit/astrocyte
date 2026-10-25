# `usage.cache.steps` reply vectors

These files are the exact bytes of `usage.cache.steps` (contract
`docs/module-surface.md`, section 4.1) answers, served by the module's own
handler, and `SHA256SUMS` lists their hashes. The data is invented (session
ids, step ids, model names and token counts); no real store was read to
produce them, and the repository is public.

A reply is one whole frame body, `{"result": ...}`, exactly as a route sends
it, with no trailing newline. The refusal frames are the module's own error
frame, `{"error":{"code":...,"message":...}}`. Requests are not published as
files: each reply's request is the params that serve it, which the test that
regenerates the reply also serves. The one exception is
`request-after-cursor.json`, a request no reply shows on its own: it is the
whole request frame, `{"method":"usage.cache.steps","params":...}`.

The test `cache_steps_vectors::published_replies_are_the_served_bytes` serves
every reply again through the real dispatch, over a store it builds in the
test, and asserts the bytes equal these files, so the vectors cannot drift
from the code. `published_sha256sums_cover_every_file` checks `SHA256SUMS`.
`every_frame_pins_the_case_its_name_names` reads the served frames and checks
that each reply and refusal still carries the case it is published for. The
request frame is checked by
`the_after_cursor_request_serves_the_steps_past_its_cursor`, which serves its
params and compares the result with `reply-streaming.json`.

The store holds five sessions, and each reply reads one of them. Every step is
written by the ingest call its harness reader makes, with that reader's
measurements: Oh My Pi (`omp`) reports input, its cache read and write, the
visible output and the reasoning count apart, and keeps its stop reason; Codex
reports an implicit cache read, no cache write, and the model's context window.
No session shares a step id with another, because the ledger would take the
second for a redelivery of the first.

- `ses_ck_phone_stream` (`omp`): three steps, so they fit the default limit of
  200 and the session has no older step. Their verdicts are `cold_start`,
  `stable`, and then `bust` with a retention of 0.376.
- `ses_ck_phone_undated` (`omp`): two steps without a time, which a reply
  counts and does not place on the timeline, and one dated step.
- `ses_ck_omp_diag` (`omp`): three steps that together carry every member an
  OMP row has.
- `ses_ck_codex_window` (`codex`): two steps that carry a `context_limit`,
  which only Codex reports.
- `ses_ck_required_only` (`omp`): one dated step with no member beyond the
  required ones, and one undated step the reply counts.

`request_body_len` and `request_size` are optional members that appear only for
a step whose source reports a request body length. The handler reports a body
length only from a source that provides one; on a prompt drop it compares two
adjacent lengths to choose `smaller` or `not_smaller`. No source reports one
today, so no vector here carries either member, and a decoder must still accept
them appearing.

## Replies

| File | Pins |
|---|---|
| `reply-streaming.json` | A whole session in one page: `harness`, `session`, `earlier_steps` false, `undated_steps` 0, and the step members `cursor`, `step_ref`, `occurred_at_ms`, `input`, `cache_read`, `cache_write`, `output`, `reasoning`, `provider`, `model`, `finish`, `prompt`, `retention`, `verdict` and `prompt_dropped`. `output` includes `reasoning` (1,200 + 500), and `cache_write` sums the split 5-minute and 1-hour writes. `context_limit` is absent, because OMP reports none. |
| `reply-earlier-steps.json` | The same session read with `limit` 1, so the page is the newest step and `earlier_steps` is true. The verdict still comes from the whole session, not from the page. |
| `reply-undated-steps.json` | `undated_steps` 2: steps without a time are counted, not placed on the timeline. |
| `reply-row-every-member.json` | Every member an OMP step can have, on three steps: both sides of a stable verdict (a retention of 0.970), a `full_bust` with a retention of 0.0, `output` as visible output plus reasoning (98 from 68 and 30), the provider, model and stop reason, and a `prompt_dropped` with no `request_size` because the reader reports no body length. |
| `reply-context-window.json` | The one member only a Codex step carries, `context_limit`, on both steps. Codex keeps no stop reason and reports no cache write, so `finish` and `cache_write` are absent, as `prompt` is the uncached input plus the cache read. No single harness carries both `finish` and `context_limit`, which is why this is a second vector: OMP gives `finish`, Codex gives `context_limit`. |
| `reply-row-required-only.json` | A step with only the members every step carries: `cursor`, `step_ref`, `occurred_at_ms`, `prompt`, `verdict` and `prompt_dropped`. No absent member appears, and none is served as zero. |

A step's members are serialised by name, so a client decodes an object by key
and must not depend on their order.

## Requests

| File | Pins |
|---|---|
| `request-after-cursor.json` | A whole request frame with `after_cursor`, the one request member no reply's request shows. Its cursor is the streaming session's first step, so serving it returns that session's second and third steps, row for row as `reply-streaming.json` holds them. |

## Refusals

| File | Request | Refusal |
|---|---|---|
| `rejected/reply-zero-limit.json` | `limit` 0 | `invalid_request`: the limit must be at least 1. |
| `rejected/reply-unknown-harness.json` | `harness` `cursor` | `unknown_harness`: only `claude-code`, `codex`, `gemini`, `kimi`, `omp`, `opencode` and `pi` are served (BROCA's step log is not served here). |
| `rejected/reply-unknown-member.json` | an extra `page` member | `malformed`: the request is a closed object, so a member it does not define is refused, never ignored. |

A request whose `harness` or `session` is missing, or whose members have the
wrong type, is refused `malformed` too. The request decoder's error text names
the member, so those frames are not published as fixed bytes.
