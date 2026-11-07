# Unit-billed catalog vector

`gpt-transcribe.json` is the exact JSON object value of the case
`catalog.get — curated unit-billed transcription rate` in
`crates/fusiform-module/fixtures/served-payloads.json` from the fusiform repository
at commit `0e18b68` (`fusiform-protocol-v0.52.0`). The enclosing case name and comma
are not part of the object. Whitespace within the object is unchanged, and there
is no added final newline.

| File | SHA-256 | Expected price |
| --- | --- | --- |
| `gpt-transcribe.json` | `3dccd68c5ffead1e9182a553c2832847e3e904b789e8d499ecf3765efc29578c` | 90 seconds costs 6,750,000 nanodollars. |

The pricing test decodes the served response, fetches the model through the real
catalog client, and commits a priced ledger charge. Unknown rounding rules remain
visible in the catalog entry rather than being converted to established absence.
