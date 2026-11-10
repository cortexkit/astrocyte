# Media catalog vectors

Each JSON file is the exact object value of one case in
`crates/fusiform-module/fixtures/served-payloads.json` in the fusiform
repository at commit `066f43e` (golden file sha256
`2df881dcd8b125befd57be105f273c3620b32000e372b483e9da0ccc150296d0`). The
enclosing case name and comma are not part of the object, whitespace within it
is unchanged, and there is no final newline. `SHA256SUMS` pins each file.

| File | Fusiform case | Rates astrocyte prices from it |
| --- | --- | --- |
| `eleven-flash-tts.json` | `catalog.get — Eleven Flash TTS` | input $0.04 per 1,000 characters (stated as `units 4, exponent 2`) |
| `gpt-image-flare.json` | `catalog.get — GPT Image Flare` | image input $8/M and image output $30/M (`exponent 0`) |
| `gemini-flash-image-split.json` | `catalog.get — Gemini Flash Image split rates` | text output $3/M and image output $60/M |
| `gemini-flash-image-alias-split.json` | `catalog.get — Gemini Flash Image alias split rates` | none: an alias inherits its rates, which the ledger leaves unpriced as `rate_inherited` |
| `gemini-flash-tts.json` | `catalog.get — Gemini Flash TTS` | audio output $9/M (`exponent 0`) |
| `gemini-omni-video.json` | `catalog.get — Gemini Omni video` | video output $12/M (`exponent 0`) |

The hand-kept rows state each rate at the exponent the vendor's page uses, not
at nanodollars. `media_rows_from_fusiform_066f43e_decode_and_price_each_role`
in `crates/astrocyte-core/src/pricing_unit_billing_tests.rs` decodes each reply
through the catalog client and commits the ledger charges.
