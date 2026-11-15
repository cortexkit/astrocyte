# Optional purchased-credit rate plan-price vector

`plan-prices.json` is the exact `response` object value of the case
`plan.prices — every curated row` in the captured Fusiform
`served-payloads.json` for `fusiform-protocol-v0.71.0` (commit `90a0531`).
The source artifact has SHA-256
`f8e20727ad654d987a9168a74c3fb85e984fadbec0e374687d8c5d0501c231ed`.
The enclosing case and request are not included. Whitespace within the response
object is unchanged, and there is no added final newline. `SHA256SUMS` pins the
captured bytes.

| File | SHA-256 | Expected price |
| --- | --- | --- |
| `plan-prices.json` | `01279f714bdb3c5cd934719c6bfc31fd40aeece0326923da43b945f524e32dbc` | Kimi Allegro is $99/month; its `fast` tier states 2× included usage but no purchased-credit rate. |

The test decodes all 13 rows through the plan-price source and resolves Allegro's
fee through the multiplier. A day's paid fee is 3,300,000,000 nanodollars, and 10%
of its weekly quota is valued at 2,310,000,000 nanodollars. Missing `credit_rate`
means the vendor states no purchased-credit rate; it does not erase the plan fee
or establish a zero rate.
