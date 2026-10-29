#!/usr/bin/env bash
# Runs CI's checks before a push, so CI confirms a change instead of
# discovering problems in it. The cargo lanes run remotely through
# `ck motor run`.
#
# Fast checks run here first and finish in seconds. Then the Linux and Windows
# suites run at the same time on the remote build servers through
# `ck motor run`. The snapshot sent is this checkout's working tree.
#
# Usage: scripts/gate.sh [--fast]
#   --fast  run only the local checks
#
# Prints `ASTROCYTE_GATE_OK <commit>` and exits 0 only when every lane passed.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
fast_only=0
[ "${1:-}" = "--fast" ] && fast_only=1

step() { printf '\n== %s\n' "$1"; }

# The same checks as CI's Linux job, in the same order, before any build.
step "train preconditions"
python3 scripts/check_train_preconditions.py
step "overview doc references"
python3 scripts/check_doc_references.py
step "nothing internal tracked under .cortexkit"
scripts/check-cortexkit-layout.sh
step "no path dependency outside the repo"
python3 scripts/check-path-deps.py
step "format"
scripts/fmt-check.sh

if [ "$fast_only" = 1 ]; then
  echo "ASTROCYTE_GATE_FAST_OK $(git rev-parse HEAD)"
  exit 0
fi

logs=$(mktemp -d "${TMPDIR:-/tmp}/astrocyte-gate.XXXXXX")
# CI installs a pinned nats-server and requires the real-bus tests to run.
# The gate installs nothing: it relies on nats-server being on the build
# servers' images, and requires the tests so a missing server fails instead
# of printing SKIP and passing.
linux_cmd='export ASTROCYTE_REQUIRE_NATS_SERVER=1
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked'
windows_cmd='export ASTROCYTE_REQUIRE_NATS_SERVER=1
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked --no-fail-fast'

step "Linux and Windows suites (remote, in parallel)"
ck motor run --vcpus 16 -- "set -euo pipefail
$linux_cmd" >"$logs/linux.log" 2>&1 &
linux_pid=$!
ck motor run --platform windows -- "set -euo pipefail
$windows_cmd" >"$logs/windows.log" 2>&1 &
windows_pid=$!

status=0
report() {
  local name=$1 code=$2
  if [ "$code" = 0 ]; then
    echo "$name: ok"
  else
    status=1
    echo "$name: FAILED (exit $code, full log $logs/$name.log)"
    grep -E '^(error|warning)|FAILED|panicked|^test result' "$logs/$name.log" | tail -n 40 || true
  fi
}
linux_code=0; wait "$linux_pid" || linux_code=$?
windows_code=0; wait "$windows_pid" || windows_code=$?
report linux "$linux_code"
report windows "$windows_code"

if [ "$status" = 0 ]; then
  echo "ASTROCYTE_GATE_OK $(git rev-parse HEAD)"
fi
exit "$status"
