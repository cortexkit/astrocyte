# Usage cache causes v1

Synthetic, content-free samples from astrocyte's real `usage.cache.steps` and
`usage.steps.read` handlers using llm-runner 0.8.55 and usage-steps 0.2.8.
The set is tagged `usage-cache-causes-v1`; published bytes are immutable.
Each reply covers account/model change, cache expiry, tools/system change,
applied view change, signed-floor movement, unexplained, and no cause.
Warm-ups are included; file rows have replaced their runner hints.
All identities, timestamps, digests and counts are invented. No store or user
data is included. Feed cursors and cache cursors are separate opaque sequences.

Regenerate an unpublished set with `ASTROCYTE_REGENERATE_CACHE_CAUSES=1 cargo test
--workspace --locked rewrite_causes_published_vectors_match_real_handlers`.
The ordinary test byte-compares both responses, SHA256SUMS and this table.

| File | SHA-256 |
| --- | --- |
| `reply-cache-steps.json` | `54b38514c7707646a52cc19bf7d70eccf5ce6702dbc8adf944914fe26206ef48` |
| `reply-feed-read.json` | `f142bb1c6eddb4a66ff1d1f8304f3e31fbbe38a4e1ae9ddd67064ab7ef864ef4` |
