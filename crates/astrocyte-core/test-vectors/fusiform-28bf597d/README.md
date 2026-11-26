# Fusiform plan price and quota pool sample

`plan-prices-synthetic-quota-pool.json` is the response object of the
`plan.prices — synthetic quota pool` golden case in Fusiform's
`crates/fusiform-module/fixtures/served-payloads.json` at commit `28bf597d`.
The copy removes the enclosing fixture and its indentation without changing
the response fields.

| File | SHA-256 |
| --- | --- |
| Upstream `served-payloads.json` | `7fb551755dc6666f5872582d5eb2235073b43c1e26dbec01ec126ce190b8ad2e` |
| `plan-prices-synthetic-quota-pool.json` | `869eaeb7b9f22a16a18c1b6b5b82973df9ae3103a2afbb14a1773e1c8bfc09a2` |

The reply dates its pool list with `resolved_at_ms`; each row also has its own
`established_at_ms`. `at_ms` is an optional request field, not a response field.
