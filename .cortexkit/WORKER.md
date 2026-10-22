# Working in astrocyte

Build and test guidance for anyone changing this repository. `ARCHITECTURE.md` explains
how the module works; `STRUCTURE.md` says where code goes.

## Gates

Run these before you commit. CI runs the same checks on Linux and Windows.

Run every `cargo` and `ckdev-mutate` command on the Linux build server (`runon: "linux,4c"`),
never on the Mac. Each freshly built test binary that runs on the Mac costs a macOS
security scan, and `ckdev-mutate check` builds every test target to list test names. Only
the Python and shell checks below run locally.

```sh
scripts/fmt-check.sh
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --workspace --all-targets --target x86_64-pc-windows-gnu -- -D warnings
cargo test --workspace
python3 scripts/check_doc_references.py
python3 scripts/check_train_preconditions.py
python3 scripts/check-path-deps.py
scripts/check-cortexkit-layout.sh
```

- Run the whole workspace (`--workspace`), not one crate with `-p`: the build cache is
  shared across workspace-wide builds, and a single-crate build misses it.
- The workspace denies lossy numeric casts (`cast_possible_truncation`, `cast_possible_wrap`,
  `cast_sign_loss`), `float_cmp` and `large_stack_arrays`. Convert with `try_from` and
  map the error. Where a cast is provably safe, put an `#[allow]` on the smallest item, with
  a comment saying why.
- `ARCHITECTURE.md` and `STRUCTURE.md` together must stay under 20,000 bytes. Trim prose;
  don't raise the ceiling.

## Tests

- Tests sit beside the code under `#[cfg(test)]`. Drive behaviour through the real path
  (the refresher drain, the served operation's dispatch), not a helper that bypasses it.
- A test for a fix must fail before the fix. Prove it: break the guard, watch the named
  test fail, then restore it.
- Assert the effect, not just the verdict. For example, assert the stored rows and the
  money, not only that a call returned an error.
- Test fakes serve until they're released, and waits have a deadline. Never assert on how
  long something took.
- A test never runs a binary named `ck-*`. Copy the build to a fresh file named
  `ckdev-astrocyte` and run the copy (see `crates/astrocyte-module/tests/version.rs`).
- Code that must work on Windows needs a test that runs in the Windows CI job. On Windows,
  a path below a regular file reports "not found", not "not a directory".

## Mutation catalogue

`mutations.toml` lists guards whose failure would be silent and costly. Each row names a
change that breaks the guard and the tests that must fail. CI replays the rows a train
touches, and every row on master. When you add a row, replay it exactly as CI does
(`ckdev-mutate run --only <row id>`). List every test the mutant reddens, or the row reports
`WRONG_TEST`.

Before delivering any change, run `ckdev-mutate check` over the whole catalogue on the build
server (about 80 s). Rows match their target code by exact text, and an edit can repeat that
text in a file another row targets. Replaying only your own rows misses that, and CI then
refuses the catalogue before running any test.

## Store and migrations

- Schema changes append a new `Migration` in `crates/astrocyte-core/src/store.rs`. Never
  edit one that has already been applied.
- `usage_event`, `measurement` and `priced_charge` are immutable. Corrections append rows.
- Never write a harness's own store. Readers open it read-only.
