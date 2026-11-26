# Cache writes billed as input: reader sample

| Reply file | Request |
| --- | --- |
| `xai-apikey.json` | `catalog.get` for `xai/grok-4.20-0309-non-reasoning`, `auth_method: "apikey"` |

The reply is the `response` from `apikey xai — cache-write fill` in Fusiform
commit `710f929f`, `crates/fusiform-module/fixtures/served-payloads.json`,
extracted without changing any values. It serves a base cache-write rule and
an above-200,000-context rule, each pointing to the corresponding input key.
