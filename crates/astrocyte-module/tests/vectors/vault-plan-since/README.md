# Vault plan start dates

These are real `credential.list_scoped` credential rows, with identity members
trimmed and no `account_id`. Their JSON bytes are pinned verbatim, followed by
one LF. They contain no secrets. The listing test supplies account IDs,
`serves: []` and `operations: ["list"]` in a reply carrying the category list
grant; the pinned client requires those listing envelope members.

| File | Tier | Start (UTC) | SHA-256 |
| --- | --- | --- | --- |
| `antigravity.json` | `ultra_5x` | 2026-09-11T00:00Z | `a3a2d074860e89ab879d75684f26f329abeffb1d2b41274f9b82b8ad0a69f1b2` |
| `chatgpt.json` | `pro_500` | 2026-11-08T00:00Z | `ddd973b88156204114105ce16d886573f0d855f62b0662d8c48f10df9395e300` |

`vault_since_vector_records_subscription_start_not_observation` applies both
rows through the vault stage and asserts the stored history timestamps. Run
`sha256sum -c SHA256SUMS` from this directory to verify the files.
