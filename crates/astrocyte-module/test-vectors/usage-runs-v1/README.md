# usage-runs-v1

Synthetic response-frame bodies for the desktop Runs cost reads. Replies are
`{"result": ...}`; refusals are `{"error":{"code":...,"message":...}}`.
No real account, session or run data is included. Identifiers use real producer
formats with synthetic values.

| File | Contract case |
|---|---|
| `list-page.json` | First page with opaque `next`, two model splits, whole-range totals |
| `list-next-page.json` | Continuation with identical range totals; Goose `session_total` |
| `list-all.json` | Session, Goose total, BROCA flow/work class and unlabelled BROCA run |
| `detail-states.json` | Priced, covered, unpriced and pending; zero versus missing quantity; inclusive output |
| `detail-run.json` | BROCA fact `segment_ref`, not a feed step; whole-run totals |
| `detail-goose.json` | Single accumulated session item; `step_ref` is producer_ref |
| `list-malformed.json` | List params do not decode |
| `list-invalid-range.json` | Empty range |
| `list-range-too-large.json` | More than 31 days |
| `list-invalid-request.json` | Invalid list limit |
| `list-not-permitted.json` | Shared feed/Run permission refusal |
| `detail-malformed.json` | Detail params do not decode |
| `detail-invalid-request.json` | Invalid detail limit |
| `detail-not-found.json` | Key has no ledger fact |
| `detail-not-permitted.json` | Shared feed/Run permission refusal |

`runs_vectors_match_real_dispatch_and_sha256sums` regenerates these bodies through
real dispatch from invented ledger facts and checks every byte and checksum.
Permission vectors use the same refusal helper as the serving handler; the fake
daemon integration tests separately verify bind-time authorization on the wire.
For regeneration, run on the build server:

```sh
ASTROCYTE_EXPORT_RUNS_VECTORS=1 cargo test --workspace runs_vectors -- --nocapture
```

Each `VECTOR` line contains a filename and exact content. Write those contents
back into this directory, then run the test without the environment variable.
`SHA256SUMS` covers all JSON frames and this README, not itself. Revisions require
a new version/tag; published tags must not move.
