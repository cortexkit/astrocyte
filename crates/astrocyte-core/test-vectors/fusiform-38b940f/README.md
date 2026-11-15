# Dated cache-lifetime catalog vectors

Each file is the exact JSON object value of one case in
`crates/fusiform-module/fixtures/served-payloads.json` in the fusiform repository
at commit `38b940f` (whole file sha256
`25704126b2d508c932706119798e2ff1dba2245fad4d21180470f684bdd038ad`). The
enclosing case name and comma are not part of the object. Whitespace within the
object is unchanged, and there is no added final newline.

These are dated reads (`at_ms` 2026-11-12T13:00Z). From that commit on, fusiform
serves cache-lifetime prices on dated reads, which the ledger uses to price a
metered call at its own time. Before it, lifetime prices came only on current
reads, so a recorded lifetime write found no dated price.

| File | Fusiform case | SHA-256 | Expected price per million tokens |
| --- | --- | --- | --- |
| `claude-opus-5-5-cache-lifetimes-dated.json` | `catalog.get — cache lifetimes dated anthropic/claude-opus-5-5` | `8ec2efa56b77326e55af4bf78a1070bdfc14882cf47f67380770f01e06f44019` | plain and 5-minute writes $5, 1-hour writes $8 |
| `gpt-6.1-sol-cache-lifetimes-dated.json` | `catalog.get — cache lifetimes dated openai/gpt-6.1-sol` | `b7ecb257fab84f646a219dbde6fb1598fe35162bff762290b3e97ddc2006930a` | refused whole as `rate_tiered`, because the model has an above-272k tier |

The tests in `crates/astrocyte-core/src/pricing_unit_billing_tests.rs` decode
each served response through the real catalog client and commit ledger charges.
