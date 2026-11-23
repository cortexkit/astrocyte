# speed-twin-1123: a fast run on a route that serves fast as its own model

Written by broca's real serializer (`broca-core` `write_attempt_fact_samples_when_requested`,
case `speed-twin-served-fast`), from a synthetic run. Not yet emitted by any live broca.

The run asked for service tier `fast` on `kimi-code-plan-global/k3.1-pro`. The catalog row names
`route.mode_wire_ids.fast = k3.1-pro-highspeed` and prices `rate.*.mode.fast`, so broca sent the
request to `k3.1-pro-highspeed` with no tier field. The twin answered, so the step reports speed
`fast`.

`speed-twin-served-fast.jsonl` holds two facts, one per line, in the bytes the usage export
stores: the run's usage segment, then the attempt. The attempt names:

- `provider` `kimi-code-plan-global`, `model` `k3.1-pro` (the base row, never the twin id);
- `requested_service_tier` `fast`;
- `served_speed` `fast`.

`charge_basis` reads `unknown` only because the test's resolver maps no Kimi key; a live run
resolves it as usual.

sha256 `04791e14146d0e01932efba005867a1e5c5cf6cd3b24ed2c0e8a1227444fbfe1`
