# Contributing to astrocyte

Thank you for contributing to astrocyte. Use the workflow below to settle scope,
reproduce problems, and verify a change before opening a pull request.

## Issue first, then a go, then a PR

1. Open an issue. Tag it `bug` or `design discussion`.
2. We reply, usually quickly. A bug with an obvious shape gets a reproduction
   and a go; anything with a design question in it is discussed there.
3. Once the issue has an explicit go, open the PR and reference the issue. The
   [pull request template](.github/pull_request_template.md) asks for it.

A PR without an issue that got a go is asked for the issue rather than
reviewed. The question "is this the right shape?" costs ten lines in an issue.
After a PR exists it costs a working implementation and review rounds, and a
reviewer looking at finished code is under quiet pressure to find a way to take
it.

## What a change here has to carry

astrocyte is a spend ledger, so a change is judged on what it can silently get
wrong, not only on what it does when it works. A PR is ready when:

- A guard asserts that the thing it prevents does not happen: the stored rows
  and the money, not only that a call returned an error. `is_err()` alone does
  not fence.
- Anything that could pass vacuously (an empty collection, a constant that
  came from a query, a fixture that lacks the case) says so or refuses.
- A bug fix comes with a test that fails without the fix. The PR names the
  test and how it failed on the unfixed code.
- Schema changes append a `Migration` in `crates/astrocyte-core/src/store.rs`.
  A migration that has been applied is never edited.
- Producer fields are stored verbatim and never inferred. Absent stays absent
  and is never written as zero.
- `usage_event`, `measurement` and `priced_charge` are immutable. A correction
  appends rows; see the [data model](docs/data-model.md).

## The gate

CI runs these checks on Linux, and clippy and the tests on Windows. Run them
before you push; the `--target x86_64-pc-windows-gnu` clippy line is the local
stand-in for the Windows job:

```sh
scripts/fmt-check.sh
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo clippy --workspace --all-targets --locked --target x86_64-pc-windows-gnu -- -D warnings
cargo test --workspace --all-targets --locked
python3 scripts/check_doc_references.py
python3 scripts/check_train_preconditions.py
python3 scripts/check-path-deps.py
scripts/check-cortexkit-layout.sh
```

- Build the whole workspace (`--workspace`), not one crate.
- The workspace denies lossy numeric casts (`cast_possible_truncation`,
  `cast_possible_wrap`, `cast_sign_loss`), `float_cmp` and
  `large_stack_arrays`. Convert with `try_from` and map the error. Where a cast
  is provably safe, put `#[allow]` on the smallest item with a comment saying
  why.
- `ARCHITECTURE.md` and `STRUCTURE.md` together stay under 20,000 bytes, and
  every path and identifier they name must exist. `check_doc_references.py`
  enforces both.
- Code that must work on Windows needs a test that runs in the Windows CI job.
- A test never runs a binary named `ck-*`. It copies the build to a fresh file
  named `ckdev-astrocyte` and runs the copy (see
  `crates/astrocyte-module/tests/version.rs`).

## How changes land

Changes land on `master` by train. `scripts/train-push.sh` pushes the change
to its own `train/**` branch, CI runs there, and `master` advances by
fast-forward only after that run is green. `scripts/check_train_preconditions.py`
checks that `.github/workflows/ci.yml` still runs on train branches.

## Ground rules that will come up in review

- **Never suppress a type error or a lint.** No deleted test to make a suite
  green.
- **A refusal names its subject.** An error message says what it is about,
  not only what would silence it.
- **Comments explain the reason for a reader with no context.** Never
  reference a task id, a chat or a review round a future reader cannot see.
- **Never write a harness's own store.** Readers open harness stores
  read-only.

## Where things are

- [ARCHITECTURE.md](ARCHITECTURE.md): how the module works.
- [STRUCTURE.md](STRUCTURE.md): where code goes, including where a new
  operation, producer or migration belongs.
- `docs/`: public contracts, architecture notes and operator guides; start with
  the [module surface](docs/module-surface.md) and the
  [data model](docs/data-model.md).
- Tests sit beside the code under `#[cfg(test)]`. Drive behaviour through the
  real path (the refresher drain, the served operation's dispatch), not a
  helper that bypasses it.
