# spend-multiplier-report-v2

Synthetic fixtures for the pool-keyed `spend.multiplier.report` reply and the
`astrocyte.multiplier` key-value (KV) bucket. The test uses real dispatch and a real NATS
publisher, with an isolated migrated store and synthetic catalog facts. No
fixture is evidence of actual plan prices, model access or calibration.

| File | Contract case |
|---|---|
| `legacy-only.json` | Plan history from before scopes were stored has a derived `pool_id`, null pool scope and no `latest` or bus rows. |
| `pool-heads.json` | Per-provider latest rows include each measured pool and the unmeasured pool-list placeholder; history also includes the legacy-only key and the prior Codex day. |
| `narrow-range.json` | `[691200000,691200001)` retains only rows ending at its lower bound; `latest` is unchanged. |
| `bus-keys.json` | Maps each raw value fixture filename to its exact three-part KV key. |
| `kv-claude-all.json` | Claude's all-models `primary` row, with `catalog_provider` set to `anthropic`. |
| `kv-claude-fable.json` | Claude's narrower `Fable` row, with the catalog provider and dated subset list. |
| `kv-gemini-flash.json` | Antigravity's Gemini Flash group, with `catalog_provider` set to `google`. |
| `kv-gemini-pro.json` | The same Antigravity plan's separate Gemini Pro group. |
| `kv-pool-list-missing.json` | An unmeasured Gemini Preview row remains a current key, but has null ratios and no `last_eligible`. |
| `kv-single-pool.json` | A single-pool Codex plan; its readings do not name a catalog provider, so `catalog_provider` is null. |

Report files are full response bodies (`{"result":{"latest":[...],"series":[...]}}`)
followed by a newline. Each `kv-*` file is the exact published JSON value with no
wrapper or newline. `pool_id` carries the raw identity while `window_pool_key`
retains its stored spelling. The bus key encodes the UTF-8 bytes of quota
provider, plan and raw pool identity as three `x`-prefixed lowercase hex parts.
Use `catalog_provider`, not the quota provider in the key, to join the pool to
the `quota_pools` list served by Fusiform's `plan.prices`. The
`spend-multiplier-report-v1` tag and fixture directory remain unchanged.

The vector test compares bytes, checksums, row semantics and read-only report
behavior. To regenerate with a writable checkout and `nats-server` on `PATH`:

```sh
ASTROCYTE_REQUIRE_NATS_SERVER=1 ASTROCYTE_EXPORT_MULTIPLIER_REPORT_VECTORS=1 cargo test --workspace --locked multiplier_report_vectors_pin_real_dispatch_and_checksums
```

On the build server, set `ASTROCYTE_EXPORT_MULTIPLIER_REPORT_VECTORS` to an
untracked writable directory and copy the exported files back. The manifest
covers the fixtures and this README, not itself. Published versions are not
updated in place.
