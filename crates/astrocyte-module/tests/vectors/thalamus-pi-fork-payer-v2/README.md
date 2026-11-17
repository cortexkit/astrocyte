# Pi fork payer rows, version 2

Reader: **Astrocyte** (ASTRO), for its row-less `fork_answer` billing intake.

These two synthetic rows were recorded on the Linux build server from committed
source `4763f105bc7c196078a80ca8ca1812c0b0db45fd`, by
`pi_head::open::tests::forks::each_answered_fork_request_is_billed_once_under_the_forks_session`
in `src/pi_head/open/tests/forks.rs`. The test drives the Pi fork take/answer routes,
drains the runner's payer outbox into a temporary payer store, and reads the stored
billing fields back. It uses no live store, login or model service.

- `fork-answer-plain.json`: a plain answer ending `end_turn`.
- `fork-answer-tool-call-refused.json`: an answer with a tool call, ending
  `tool_call_refused`; the tool is not run. Its billing outcome is still `completed`
  because the model request completed and reported usage.

Both requests belong to the same declared Pi fork session and have distinct attempt
ids. The account (`acct-head`), credential (`cred-head`), model (`pi-model`) and
provider (`test`) are synthetic. The account and charge basis come from the head's
synthetic billing row. `step_ref` is null: neither request has a transcript row.
`usage_json` remains a JSON string, matching the payer store's TEXT field. Each model
entry carries **10 five-minute** and **20 one-hour** cache-write tokens beside the
**30-token total**, plus 12 input, 900 cached-input and 7 output tokens.

The files were inspected for personal data. They contain only the synthetic labels
above, generated fork/attempt ids, a fixed test timestamp and token counts: no
prompts, answer text, filesystem paths, personal identifiers, login material or
credentials. No previous sample set was changed.

`SHA256SUMS` pins the exact bytes of the two JSON files, including trailing newlines.
It does not cover this README. Verify it from this directory with
`sha256sum -c SHA256SUMS`.

## Recording a new version

Run from a clean, committed checkout on the Linux build server, naming a new output
directory rather than overwriting these samples:

```sh
THALAMUS_PI_FORK_PAYER_SAMPLES=$PWD/crates/thalamus-harness-module/test-vectors/pi-fork-payer-v3 \
  cargo test --locked -p thalamus-harness-module --features test-support --lib \
  pi_head::open::tests::forks::each_answered_fork_request_is_billed_once_under_the_forks_session \
  -- --exact
```

Return both JSON files from the server, generate a new checksum manifest and record
the source commit. Fork and attempt ids are generated, so a new recording has new
bytes even when its billing facts are unchanged.
