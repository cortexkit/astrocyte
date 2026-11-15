# analytics-quota-history-v1

Synthetic `spend.quota.history` response bodies, regenerated through the real
reader-backed handler over a leased synthetic store. No real account or
credential identifiers are included. Successful bodies are `{"result": ...}`;
refusals are `{"error":{"code":...,"message":...}}`.

| File | Contract case |
|---|---|
| `history-hour.json` | Two account windows, raw percentages above 100, a reset at the first sample, and an account-less credential series |
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
by the handler. To regenerate, run on the build server:

```sh
ASTROCYTE_EXPORT_QUOTA_HISTORY_VECTORS=1 cargo test --workspace --locked quota_history_vectors
```

The export writes the JSON bodies and `SHA256SUMS` into this directory. Request
those files as returned paths when running remotely. Run the test again without
the export variable to verify the pinned files. `SHA256SUMS` covers each JSON
body and this README, not itself. Published version tags must not move.
