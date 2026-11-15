# Fusiform served rate history v1

These byte-for-byte live replies were captured on 2026-11-15 from Fusiform
build `ea8dfef`, using:

```sh
ck models history --provider <provider> --model <model> --fact-prefix rate. --served --json
```

The CLI prints the `catalog.history` reply, decoded in tests and in the wire
client as `fusiform-protocol =0.71.0`'s `HistoryResponse`. `SHA256SUMS` pins the
original bytes. The SHA-256 of `SHA256SUMS` is
`0e2f08bf6a07b4614fdc1b116361b2d4d5d27698b93df0640ec55d50edd73177`.

| File | Coverage |
| --- | --- |
| `anthropic--claude-opus-5-5.json` | Lifetime keys at the 2026-11-07 boundary and fast mode |
| `openai--gpt-6.1-sol.json` | Context thresholds, lifetime and mode keys |
| `kimi-code-plan-global--k3.1-pro-highspeed.json` | Plan row, ordinary read |

None of these captures contains an intra-hour revert or a refused segment.
Those cases use explicitly synthetic intervals in `rate_era/tests.rs`.
Do not regenerate or reshape these live replies; a future capture is a new set.
