# Host parity wave 1 reader samples

Recorded from commit `bf0307711cdfa389ab01a5699e4161b2d1f80a82` with focused
`thalamus-harness-module` tests on Linux. These are deterministic runner fixtures, not live
accounts or provider captures. The six original recording tests passed (6/6); the two tests
used to capture the read/history pages passed (2/2). The page capture used a scratch-only
recorder, leaving the committed sources unchanged.

## Run results

- `opencode2-run-result.json`: an ordinary completed OpenCode 2 run; `final_message` is the
  last host assistant reply.
- `opencode2-run-result-empty.json`: a completed OpenCode 2 run whose host stored no reply.
- `pi-run-result.json`: the corresponding Pi case.
- `claude-code-run-result.json`: the corresponding Claude Code case, reached through a
  non-API synthetic row (`No response requested.`). In all three no-row cases the sealing
  transaction writes one `thalamus-final-…` assistant entry with `text: ""`; it is not a
  person’s prompt, a notice, or an earlier reply. Every empty-entry run result has
  `steps: 0`, `unreported_steps: 0`, and an empty `by_model` list.

A run sealed before any transcript intake gets no empty entry. If a real reply arrives
after an empty entry, the real reply becomes `final_message`; the empty entry stays
readable before it and contributes no step or usage.

The practical no-row host case found is Claude Code’s non-API synthetic reply. The pinned
Claude 2.1.318 session fixture contains one such row. The repository does not contain a
frequency measurement for how often that row is terminal in production, so no live-rate
claim is made. New seals write at most one empty entry per run; legacy stored runs are not
repaired by reads.

## Empty replies on app-facing read surfaces

The two `*-session-read-display-empty.json` files are `session.read` responses with
`include_display_facts: true`. The two `*-session-history-display-empty.json` files are
`session.history` responses with `form: "display"`. Each page ends with the completed run’s
`thalamus-final-…` entry.

- **OpenCode 2:** both surfaces say `role: "assistant"`. Read has `message.content: []` and
  `message.metadata.cortexkit_runner_entry: true`; display history has `row.parts: []` and
  `row.text: ""`. History also has `output_refs.count: 0` and
  `output_refs.set_id: "empty-reply:<mid>"`.
- **Claude Code:** both surfaces say `role: "assistant"`. Read has `message.content: []` and
  top-level `message.cortexkit_runner_entry: true`; display history has `row.parts: []` and
  `row.text: ""`. The preceding native synthetic row still says `No response requested.`
  with one text part; the runner-written empty row is separate.

Neither response schema has an `origin` member. The entry’s stored origin is `runner`; the
read surfaces expose that only through `cortexkit_runner_entry` in the raw message and the
`thalamus-final-…` id. Display-history projection carries no runner-origin or host-origin
marker. OpenCode’s raw message puts the runner mark under `metadata`; Claude’s puts it at
the message’s top level.

## Pi evidence

- `pi-step-finished-zero.json`: a decoded `step.finished.v1` body with
  `cache_write_tokens: 0` and `reasoning_tokens: 0` preserved.
- `pi-run-status-provider-error.json`: a `run.status` whose 429 is
  `transient` / `rate_limit`, with `rate_limit_error` retained as the detail code.

Pinned `@earendil-works/pi-ai@1.6.0` keeps HTTP status in the two
`formatProviderError` prefixes of `message.errorMessage` (`429: <body>` or
`OpenAI API error (429): <body>`), not a separate field. The provider’s `error.type` and
`error.code` are inside that message’s JSON body. Only the retry-cap path retains a delay,
as `Server requested Ns retry delay`; ordinary consumed Retry-After headers are not
journaled.

All ten JSON bodies were read after recording. They contain synthetic prompts, model names,
opaque fixture identifiers, and no personal names, paths, credentials, account details, or
other personal data. `SHA256SUMS` covers every JSON file and this README.
