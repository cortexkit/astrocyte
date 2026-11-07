# Cache-lifetime catalog vectors

Each file is the exact JSON object value of one case in
`crates/fusiform-module/fixtures/served-payloads.json` in the fusiform repository
at commit `48b2d77` (fusiform-protocol 0.54.0, whose rate type is unchanged from
0.52.0). The enclosing case name and comma are not part of the object. Whitespace
within the object is unchanged, and there is no added final newline.

| File | Fusiform case | SHA-256 | Expected price per million tokens |
| --- | --- | --- | --- |
| `claude-opus-5-5-cache-lifetimes.json` | `catalog.get — cache lifetimes anthropic/claude-opus-5-5` | `1c6d423bc6821633b404e6cdd39f77a864064359a3b4829033c74b0abfb2e61b` | plain and 5-minute writes $5, 1-hour writes $8 |
| `gpt-6.1-sol-cache-lifetimes.json` | `catalog.get — cache lifetimes openai/gpt-6.1-sol` | `c302a523f188ac031c84abff027a119845393357d76c5add979564afc7c827a5` | refused whole as `rate_tiered`, because the model has an above-272k tier |

The tests in `crates/astrocyte-core/src/pricing_unit_billing_tests.rs` decode
each served response through the real catalog client and commit ledger charges.
