# broca step events with rewrite-cause facts (llm-runner 0.8.55), 22 Nov

Each file is one `step.finished.v1` bus payload, serialized by broca's real publisher
(`serde_json::to_vec` of the contract's `StepFinished`), from synthetic test rigs.

- `rewrite-facts-step-7.json`, `rewrite-facts-step-8.json`: one run, two steps, with an account
  move between them. Step 7 names `account_ref` for account-a (the contract's known answer
  for account-a/credential-a), `view_version: 3`, `signed_floor`. Step 8 names the moved-to
  account-b, `view_version: 0` (its request carried no view). Both steps send the same tools and
  system text on the same route, so `tools_digest` and `system_digest` are equal across them.
- `completed-*`, `interrupted-*`, `keep-warm-*`: the existing bus samples, now with `warm_up`
  (`false` for a step broca ran, `true` for a keep-warm refresh), `tools_digest` and
  `system_digest`. No routed account and no view in those rigs, so `account_ref`,
  `view_version` and `signed_floor` are absent, never null.

Absent means broca couldn't tell; it never writes a guessed value. `warm_up` is `false` on every
step event, `true` only on a keep-warm refresh.
