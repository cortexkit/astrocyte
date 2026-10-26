# Working in astrocyte

Build and test guidance for anyone changing this repository. `ARCHITECTURE.md` explains
how the module works; `STRUCTURE.md` says where code goes.

## Gates

A worker runs fmt, both clippy passes, the tests for what it touched (filtered by
name), and the Python and shell checks, then delivers. The head runs the full suite once
per landing, and CI runs everything on Linux and Windows.

Run every `cargo` command on the Linux build server (`runon: "linux,4c"`),
never on the shared development machine. Local builds and tests compete with every other
agent on that machine (on macOS, each freshly built test binary also costs a security
scan). Only the Python and shell checks below run locally.

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
- A bug fix comes with a test that fails without the fix. Show that once (run it on the
  unfixed code and name the failure) and report it. No break lists, mutation rows or
  repeat runs.
- Assert the effect, not just the verdict. For example, assert the stored rows and the
  money, not only that a call returned an error.
- Test fakes serve until they're released, and waits have a deadline. Never assert on how
  long something took.
- A test never runs a binary named `ck-*`. Copy the build to a fresh file named
  `ckdev-astrocyte` and run the copy (see `crates/astrocyte-module/tests/version.rs`).
- Code that must work on Windows needs a test that runs in the Windows CI job. On Windows,
  a path below a regular file reports "not found", not "not a directory".

## Store and migrations

- Schema changes append a new `Migration` in `crates/astrocyte-core/src/store.rs`. Never
  edit one that has already been applied.
- `usage_event`, `measurement` and `priced_charge` are immutable. Corrections append rows.
- Never write a harness's own store. Readers open it read-only.
