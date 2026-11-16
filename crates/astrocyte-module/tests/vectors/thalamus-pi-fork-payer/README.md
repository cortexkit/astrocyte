# pi-fork-payer-v1

Two `payer_record` rows of `request_kind: "fork_answer"` as the module writes them for a
fork's model request on a Pi head, read back from `payer.db` with every column but `seq`.

- `fork-answer-plain.json`: an answer that ended its turn (`end_turn`).
- `fork-answer-tool-call-refused.json`: an answer with a tool call, which the fork
  answered `tool_call_refused` (the provider's request itself completed, with usage).

Recorded from commit 5a8b5ceb6f22948aed0a7af1cfba2aea7a1c0a56 by the stand-in test
`pi_head::open::tests::forks::each_answered_fork_request_is_billed_once_under_the_forks_session`
with `THALAMUS_PI_FORK_PAYER_SAMPLES` set. The account, login and charge basis are copied
from the head's newest payer row (`cred-head`, `acct-head`, `subscription_included` in
the test); the session is the fork's own (`pi-fork-<id>`), declared in `payer_session`
under harness `pi` and the head's owner before the first row. `recorded_at_ms` is the
test's fixed clock.
