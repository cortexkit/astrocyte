# Runs change watch, version 1

Synthetic held-watch frame bodies. A notice says only that clients should re-read
Runs, never that a particular call or charge exists. The cursor is process-local.

| File | Meaning |
| --- | --- |
| request.json | Open a watch without filters. |
| notice.json | One newly committed synthetic call, no charges or summary publications. |
| not-permitted.json | A caller outside the Runs-read admission set is refused. |

`runs_subscribe_vectors` checks the request, generated token, permission refusal
and SHA-256 checksums against these bytes.
