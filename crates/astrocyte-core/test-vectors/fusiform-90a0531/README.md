# Speed-twin catalog and plan-price vectors

Each JSON file is the exact object value of one case in
`crates/fusiform-module/fixtures/served-payloads.json` in the fusiform
repository at commit `90a0531e36a8ba1023e1e73e4818ffa07b1657cb` (golden file
sha256 `a6a54fee153aec86bf8bd8b7ef50e6e2ad94360e93b66736152259af9dc681c8`).
The enclosing case name and comma are not part of the object, whitespace within
it is unchanged, and there is no final newline. `SHA256SUMS` pins each file.
The golden replies use fusiform-protocol 0.71.0; these tests exercise astrocyte's
unchanged 0.52.0 dependency, not a compatibility adapter.

| File | Fusiform case | Observed result |
| --- | --- | --- |
| `k3.1-pro-plain.json` | `catalog.get — speed twin k3.1-pro plain` | decodes; input and output are unpriced with `stated_zero_requires_policy` |
| `k3.1-pro-apikey.json` | `catalog.get — speed twin k3.1-pro apikey` | decodes; selects base input $2/M and output $10/M, not `mode.fast`; both unpriced with `rate_inherited` |
| `k3.1-pro-highspeed-plain.json` | `catalog.get — speed twin k3.1-pro-highspeed plain` | decodes; input and output are unpriced with `stated_zero_requires_policy` |
| `k3.1-pro-highspeed-apikey.json` | `catalog.get — speed twin k3.1-pro-highspeed apikey` | decodes; selects the twin's input $4/M and output $20/M; both unpriced with `rate_inherited` |
| `plan-prices-every-curated-row.json` | `plan.prices — every curated row` | decodes; all 13 plans resolve with the prices or explicit unpriced reasons below |

## Catalog decoding and exact ledger charges

The four `speed_twin_from_fusiform_90a0531_*` tests in
`crates/astrocyte-core/src/pricing_unit_billing_tests.rs` use the same digest,
`CatalogGetResponse`, fake transport, real catalog client and `reprice_event`
path as `media_rows_from_fusiform_066f43e_decode_and_price_each_role`. They keep
the new top-level `speed_twins` array and the `route.mode_wire_ids`,
`route.mode_labels` and `model.mode_of` facts in the pinned bytes. All four
replies decode without a refusal.

Each test records a `metered_api` event under `kimi-code-plan-global` for the
fixture's model, with 1,000,000 input tokens and 1,000,000 output tokens, and no
recorded mode. The event instant `1794700800000` precedes the catalog's apikey
quota-proxy view deadline (`plane.review_by_ms`,
`1797552000000`) and the twin-mapping deadline (`speed_twins[0].review_by_ms`,
`1799884800000`). These replies are therefore not past their review deadlines.
The stored input and output `charge_nanos` are **SQL NULL in every case**, not
zero and not a priced charge.

For the highspeed apikey case, the selected rate values are exactly
`4000000000` and `20000000000`, exponent 9: nominally $4 and $20 for these
quantities. For the base apikey case they are `2000000000` and `10000000000`,
exponent 9: nominally $2 and $10. The tests assert these values and the stored
`rate.input` / `rate.output` keys, so the base event cannot accidentally select
the fast rates. However, every apikey rate has
`inherited_from.basis = "quota_proxy"`. Astrocyte's existing pricing match in
`crates/astrocyte-core/src/pricing.rs` leaves all inherited rates unpriced as
`rate_inherited`, regardless of basis. The expected $4/$20 charged outcome is
therefore **not supported by this production pricing path**. The plain replies
instead contain `stated_zero` and produce `stated_zero_requires_policy` for
both roles. No production behavior or rate provenance was changed to obtain
these results.

## Plan-price decoding and multiplier valuation

`plan_prices_from_fusiform_90a0531_decode_and_price_every_plan` in
`crates/astrocyte-core/src/multiplier/v2_tests.rs` verifies the digest and
decodes the complete response as `PlanPricesResponse`. It asserts that the
Kimi Allegro row has no `service_tiers` entry, then resolves all 13 listed
plans through `resolve_plans` / `fusiform_price` at `1794700800000`.
The expected money is stated independently of the fixture's representation.

| Plan | Monthly nanodollars | Result |
| --- | --- | --- |
| `anthropic/max_20x` | `200000000000` | $200 from fusiform |
| `anthropic/max_5x` | `100000000000` | $100 from fusiform |
| `anthropic/pro` | `20000000000` | $20 from fusiform |
| `google/ultra_5x` | `99990000000` | $99.99 from fusiform |
| `kimi-code-plan-global/allegro` | `99000000000` | $99 from fusiform |
| `meta/power_usage` | `50000000000` | $50 from fusiform |
| `ollama-cloud/free` | none | `plan_price_out_of_range`: the multiplier requires a positive monthly fee |
| `openai/plus` | `20000000000` | $20 from fusiform |
| `openai/pro` | none | `plan_price_refused`: the served row says the API plan string does not identify one vendor tier |
| `openai/pro_100` | `100000000000` | $100 from fusiform |
| `openai/pro_200` | `200000000000` | $200 from fusiform |
| `openai/pro_500` | `500000000000` | $500 from fusiform |
| `opencode-go/go` | `10000000000` | $10 from fusiform |

All 11 positive fees have source `fusiform` and are within their review
deadlines. The two unpriced results are per-plan decisions after successful
decoding, not failures of the reply. The four catalog case values are
byte-identical to the earlier golden; only the plan-price case changed.

## Earlier 08af2b0 refusal and its scope

Before the restage, commit `08af2b0edf709fc6c1dc9e9c8239f537b3e00d75`
served a `kimi-code-plan-global/allegro` fast tier with
`included_usage_rate: {units: 2, exponent: 0}` and no `credit_rate`. Its golden
file sha256 was
`f8e20727ad654d987a9168a74c3fb85e984fadbec0e374687d8c5d0501c231ed`,
and the extracted plan-price object sha256 was
`352e5e3e19bc2be03310e21148c4ccc445f967b8ec5ece6d0a7f962cc1b48733`.
Against the unchanged astrocyte dependency, both that tier decoded as
`ServiceTierWire` and the whole response decoded as `PlanPricesResponse`
were observed to refuse with the exact serde error:

```text
missing field `credit_rate`
```

The module's `call_plan_prices` in
`crates/astrocyte-module/src/fusiform_client.rs` deserializes the entire payload
once. With the earlier reply it returned `CatalogCallError` code
`undecodable` and message:

```text
fusiform plan.prices result: missing field `credit_rate`
```

The earlier fixture test mirrored that decoding boundary in a raw fixture
source and called `resolve_plans`. In
`crates/astrocyte-core/src/multiplier.rs`, resolution fetches the response
before iterating accounts or calling `fusiform_price`. The earlier reply
returned `MultiplierError::PlanPrices` containing exactly:

```text
plan.prices: undecodable: fusiform plan.prices result: missing field `credit_rate`
```

There is no per-row recovery: the bad Kimi tier made **all plan prices in
that served reply unavailable**, including otherwise decodable providers.
As a control, the earlier test decoded the unchanged `anthropic/pro` row
alone and priced it through `resolve_plans` / `fusiform_price` at exactly
`20000000000` nanodollars per month ($20). The full earlier reply failed
even when that plan was requested. Resolution whose plans all have configured
overrides bypasses the source entirely and is not affected by a decode failure.

The vault-plan reconciler in
`crates/astrocyte-module/src/vault_source.rs` maps any source error to
`plan_prices_unavailable`, sets `price_available` false, and substitutes a
`PlanPricesResponse` with an empty `prices` vector. It publishes
`plan_price_status: "unavailable"`. In
`crates/astrocyte-core/src/vault_plan.rs`, `prepare_reconciliation` then lists
known eligible plan keys as unpriced because `ledger_price` finds no row.
That fallback is reply-wide, not just a refusal of the Kimi row; it does not imply that
previously recorded ledger charges are deleted. Plans that remain unpriced
are retried hourly. The vault-reconcile consequences are established by
reading these existing branches, not by changing or bypassing the decoder.
The restage removed the incompatible Allegro `service_tiers` entry upstream;
no astrocyte production code or decoding policy was changed.
