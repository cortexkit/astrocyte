# Paid Anthropic API keys through routing (broca design 32, pool half)

A paid Anthropic key is filed in the vault under the route provider `anthropic-platform`, with
`serves: ["anthropic"]`. The model catalog has no `anthropic-platform` rows. From this build, a
run on `anthropic-platform/<model>` borrows the catalog rows of `anthropic`, the one provider
its credentials serve. Before this build such a send was refused at the plan check and at
admission ("not a supported model").

Produced by broca's real serializers in the test
`a_pool_key_run_borrows_anthropic_rows_and_renders_the_same_request` (one synthetic pool key
`apikey:anthropic:acct1`, an HTTP stand-in for Anthropic's host, model `claude-haiku-4-5`).

Files:
- `pool-run-export-fact-segment.json`: the usage export fact's segment (`export_facts.segment_json`).
  `provider` is `anthropic`, the provider Astrocyte prices, never `anthropic-platform`.
  `auth_selection` is broca's routing key `apikey:anthropic-platform`; `resolved_credential_id`
  is the pool key that paid. `charge_basis` is `metered_api`.
- `pool-run-frozen-run-config.json`: the run's frozen config. `model.provider_module_id` stays
  `anthropic-platform` (what the caller named). The new optional member `served_provider:
  "anthropic"` records the borrowed provider; it is absent on every other run.
- `pool-run-request-body.json`: the request Anthropic receives. The test asserts it is
  byte-identical to the same send on `anthropic/claude-haiku-4-5` with a plain key, cache
  markers included, sent with `x-api-key` and no Claude Code persona.

What does not change: `run.status`, `run.result`, `session.head`, `send.status` and subscribe
frames gain no member, and none names the borrowed provider (a single-model run's usage
carries no provider). Step events for these runs say `provider: "anthropic"`.
