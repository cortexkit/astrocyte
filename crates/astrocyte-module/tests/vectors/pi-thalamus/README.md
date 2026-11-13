# Synthetic THALAMUS Pi session (v1)

These files contain no personal data. `session.jsonl` is a Pi session header and
one assistant response with explicit input, output, reasoning and cache usage.
`roots.json` declares a done folder before THALAMUS has learned the native id.
The source tests replace only its synthetic absolute `path` with their temporary
folder; the native header id and all usage bytes are unchanged.

| File | SHA-256 |
|---|---|
| `roots.json` | `599c836daf4dc78a82bf37a848e295e76fd403cb70e77a259028bd68d94f2603` |
| `session.jsonl` | `ac7d32053fe552cb002aeaef2b26b78d4d0a85afbcd47dc808381e5a13dca1ea` |

The expected call reference is `pi:response:pi-synthetic-response`, native session
is `pi-synthetic-one`, recorded session is `core-conversation`, and work class is
`synthetic-owner`. Input is 11 tokens; total output 7 includes 2 reasoning tokens,
so normalized output is 5. Cache read and write are 13 and 17 tokens respectively.
No payer is invented by the fixture; tests insert synthetic payer facts when
exercising the join or wait bound. A real recorded THALAMUS sample belongs in a
separate addition once available.

To verify the pinned bytes, run `sha256sum -c SHA256SUMS` in this folder.
The files are also hash-checked by the source tests. To deliberately regenerate
the synthetic fixture, edit the JSON or JSONL locally, calculate its SHA-256,
and update `SHA256SUMS`, this table and the test's literal digest together.
