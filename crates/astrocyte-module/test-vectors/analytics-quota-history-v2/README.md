# analytics-quota-history-v2

Synthetic `spend.quota.history` response bodies, regenerated through the real
reader-backed handler over a leased synthetic store. No real account or
credential identifiers are included. Successful bodies are `{"result": ...}`;
refusals are `{"error":{"code":...,"message":...}}`.

Version 2 replaces v1's percent-drop resets with allowance-period resets from
recorded reset times and includes a one-point drop that is not a reset.

| File | Contract case |
|---|---|
| `history-hour.json` | Two account windows, raw percentages above 100, one allowance-period reset at the first sample, a one-point drop within that period, and an account-less credential series |
| `history-day-credential.json` | UTC day widening and quota-provider filtering, including filtered earliest history |
| `invalid-range.json` | Empty range |
| `hour-range-too-large.json` | More than 31 days at hour grain |
| `day-range-too-large.json` | More than 400 days at day grain |
| `invalid-filter.json` | Empty provider filter |
| `malformed-member.json` | Unknown request member |
| `malformed-grain.json` | Unsupported grain |
| `malformed-required.json` | Missing required upper bound |

`quota_history_vectors_pin_real_handler_and_checksums` checks every byte and
the SHA-256 entries, with semantic assertions on the data and no store writes
by the handler. To regenerate, run from the repository root on a Linux build
machine with this workspace's Rust toolchain:

```sh
ASTROCYTE_EXPORT_QUOTA_HISTORY_VECTORS=1 cargo test --workspace --locked quota_history_vectors
```

The export writes the JSON bodies and `SHA256SUMS` into this directory. A build
machine writes them into its own checkout, so return the JSON bodies and
`SHA256SUMS` as build artifacts before committing. If the checkout is read-only,
the verification assertion prints the actual and expected bytes; decode the
handler's expected bytes locally and recompute `SHA256SUMS`.
Run the test again without the export variable to verify the
pinned files. `SHA256SUMS` covers each JSON
body and this README, not itself. Published version tags must not move.
