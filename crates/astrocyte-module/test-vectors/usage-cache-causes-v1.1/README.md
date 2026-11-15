# Usage cache causes v1.1

Synthetic, content-free samples from astrocyte's real `usage.cache.steps` and
`usage.steps.read` handlers using llm-runner 0.8.55 and usage-steps 0.2.8.
This set supersedes `usage-cache-causes-v1`, whose tag keeps its original bytes.
Version 1.1 adds retained evidence to cache steps and a row with expired evidence but a kept cause.
Each reply covers account/model change, cache expiry, tools/system change,
applied view change, a changed `signed_floor` identifier, unexplained, and no cause.
Warm-ups are included. Durable file-read rows replace the same steps' earlier live runner-event rows.
All identities, timestamps, digests and counts are invented. No store or user
data is included. Feed cursors and cache cursors are separate opaque sequences.

Regenerate an unpublished set with `ASTROCYTE_REGENERATE_CACHE_CAUSES=1 cargo test
--workspace --locked rewrite_causes_published_vectors_match_real_handlers`.
The ordinary test byte-compares both responses, SHA256SUMS and this table.

| File | SHA-256 |
| --- | --- |
| `reply-cache-steps.json` | `b8f0607cbfb50dfb30fd1cd39b7b30fef207b83c6e122746c29da26a75a07459` |
| `reply-feed-read.json` | `4ab13fb0238c0f77098ea8008605f514e999604d841025b7783804d4b78e6b70` |
