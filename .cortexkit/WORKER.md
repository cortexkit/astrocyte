# Working in astrocyte

Build and test guidance for anyone changing this repository. `ARCHITECTURE.md` explains
how the module works; `STRUCTURE.md` says where code goes.

## Gates

`scripts/gate.sh` runs CI's checks. Its local checks take about 4 seconds:
the train preconditions, the doc references, the `.cortexkit` layout, the path
dependencies and the format. It then runs the full Linux and Windows suites in parallel
on the build servers, and prints `ASTROCYTE_GATE_OK <commit>` only when everything
passed. Run it before every push. A delivery must report a passing full gate, not
filtered tests.

Never run `cargo` on the shared development machine. Local builds compete with all other
work there, and on macOS each freshly built test binary also costs a security scan. If
`ck motor run` is unavailable from your shell, run the same cargo lanes on the Linux and
Windows build servers another way (for example the shell tool's `runon` setting):

```sh
scripts/gate.sh --fast                                   # local, seconds
# runon: "linux,16c"
ASTROCYTE_REQUIRE_NATS_SERVER=1 cargo clippy --workspace --all-targets --locked -- -D warnings
ASTROCYTE_REQUIRE_NATS_SERVER=1 cargo test --workspace --all-targets --locked
# runon: "windows"
ASTROCYTE_REQUIRE_NATS_SERVER=1 cargo clippy --workspace --all-targets --locked -- -D warnings
ASTROCYTE_REQUIRE_NATS_SERVER=1 cargo test --workspace --all-targets --locked --no-fail-fast
```

`ASTROCYTE_REQUIRE_NATS_SERVER=1` makes the real-bus tests fail rather than skip when
nats-server is missing, as in CI. CI installs a pinned server; the gate relies on the
build servers' images having one.

## Standing approvals

Do these without asking, and say in your report that you did:

- Build and test on the remote build servers, at whatever vCPU count the job needs.
- Regenerate a test vector or golden file when its regenerating test shows the only
  difference is the change you were asked to make. Update its `SHA256SUMS` and README
  table in the same commit. A published, tagged vector set is never edited; a new
  schema version gets a new folder and tag.
- Add a test-only seam behind `#[cfg(test)]` or the `test-support` feature.
- Make a fake peer serve until it is released, or give a wait a deadline.
- Keep `ARCHITECTURE.md` and `STRUCTURE.md` under their size cap by moving detail into
  the matching page under `docs/`, in plain full sentences. Never compress them into
  fragments.
- Add `.gitattributes` rules that keep test-vector bytes identical on Windows.
- Rebase onto `origin/master` and resolve conflicts keeping both sides' intent.

Ask first, because these are contracts or money:

- A served operation's request or reply shape, a refusal code, or a health rule.
- A config member, or decoding of stored config generations.
- A migration that rewrites or deletes existing rows (adding tables, columns or
  indexes is fine).
- Event identity, pricing, charge basis, or any recorded money.
- What `docs/module-surface.md` or a contract under `docs/` promises.

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
