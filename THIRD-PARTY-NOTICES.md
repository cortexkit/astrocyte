# Third-party notices

This file records notices for code adapted in astrocyte. astrocyte itself is
licensed under the MIT License in [LICENSE](LICENSE). Dependencies retain their
own licenses.

## tokscale — MIT

Source: https://github.com/junhoyeo/tokscale, release v4.17.0 (tag commit
`dcf8d3656bbcecf05d112c6f35f9be328e4f66b7`), crate `crates/tokscale-core`.

Selected reader behavior is also adapted from release v4.20.0 (tag commit
`c1973901fc2f7ba0d134252bfd923c714cc123db`): bounded Codex archive reads,
request-usage settlement and missing-usage diagnostics, Pi billed operations,
and Codestral/Devstral provider inference. Astrocyte retains its own immutable
event identities, optional measurements and ingestion scheduling.

These files in `crates/astrocyte-readers/src/` are copied from the
same-named files under tokscale's `crates/tokscale-core/src/`. Each says so in
its header, and each marks where astrocyte departs from upstream with an
`ASTROCYTE DIVERGENCE` comment:

- `lib.rs`
- `provider_identity.rs`
- `sessions/mod.rs`
- `sessions/utils.rs`
- `sessions/claudecode.rs`
- `sessions/codex.rs`
- `sessions/gemini.rs`
- `sessions/goose.rs`
- `sessions/kimi.rs`
- `sessions/omp.rs`
- `sessions/opencode.rs`
- `sessions/opencode_schema.rs`
- `sessions/pi.rs`

`sessions/jsonl_resume.rs` is astrocyte code. Its prefix check follows the
approach of tokscale's Codex incremental cache (`codex_prefix_matches` in
`message_cache.rs`); no code is copied from it. `sessions/roles.rs`,
`sessions/sqlite_read.rs`, `file_identity.rs` and `sqlite_uri.rs` are astrocyte
code.

The full upstream license is in
[crates/astrocyte-readers/LICENSE-tokscale](crates/astrocyte-readers/LICENSE-tokscale),
which the file headers reference by relative path:

```text
MIT License

Copyright (c) 2025 Junho Yeo

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
