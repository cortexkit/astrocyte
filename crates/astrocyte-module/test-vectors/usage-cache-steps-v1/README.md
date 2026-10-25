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
that each still carries the case it is published for.

The store holds four sessions, and each reply reads one of them:

- `ses_ck_phone_stream` (`pi`): three steps, so they fit the default limit of
  200 and the session has no older step. Their verdicts are `cold_start`,
  `stable`, and then `bust` with a retention of 0.376.
- `ses_ck_phone_undated` (`pi`): two steps without a time, which a reply
  counts and does not place on the timeline, and one dated step.
- `ses_ck_broca_diag` (`broca`): three BROCA diagnostic steps. They are the
  only steps with `request_body_len`, and the third drops the prompt, so it
  carries `request_size`.
- `ses_ck_required_only` (`pi`): one dated step with no member beyond the
  required ones, and one undated step the reply counts.

## Replies

| File | Pins |
|---|---|
| `reply-streaming.json` | A whole session in one page: `harness`, `session`, `earlier_steps` false, `undated_steps` 0, and the step members `cursor`, `step_ref`, `occurred_at_ms`, `input`, `cache_read`, `cache_write`, `output`, `reasoning`, `provider`, `model`, `finish`, `context_limit`, `prompt`, `retention`, `verdict` and `prompt_dropped`. `output` includes `reasoning` (1,200 + 500), and `cache_write` sums the split 5-minute and 1-hour writes. |
| `reply-earlier-steps.json` | The same session read with `limit` 1, so the page is the newest step and `earlier_steps` is true. The verdict still comes from the whole session, not from the page. |
| `reply-undated-steps.json` | `undated_steps` 2: steps without a time are counted, not placed on the timeline. |
| `reply-row-every-member.json` | Every member the handler can emit for a step: those of `reply-streaming.json` plus `request_body_len` and `request_size`. The third step has `prompt_dropped` true and `request_size` `smaller`, and a retention above 0. `not_smaller` (a prompt drop whose request body did not shrink) is not shown, because a drop needs tens of thousands of tokens fewer and the invented bodies stay small; a decoder must still accept it. |
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
| `rejected/reply-unknown-harness.json` | `harness` `cursor` | `unknown_harness`: only `broca`, `claude-code`, `codex`, `gemini`, `kimi`, `omp`, `opencode` and `pi` are served. |
| `rejected/reply-unknown-member.json` | an extra `page` member | `malformed`: the request is a closed object, so a member it does not define is refused, never ignored. |

A request whose `harness` or `session` is missing, or whose members have the
wrong type, is refused `malformed` too. The decoder's text names the member, so
those frames are not published as fixed bytes.
