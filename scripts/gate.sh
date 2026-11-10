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
# A native crash in a test's child process (an access violation) leaves no
# stderr, only a minidump that Windows Error Reporting writes to C:\CrashDumps a
# few seconds after the process dies. When the suite fails, the job waits up to
# 60 s for a dump written during this job and copies it to the checkout root to
# be returned, so the faulting module can be read afterwards. In a Windows job
# `target/` links out of the checkout, and a returned file must sit inside it.
windows_cmd='export ASTROCYTE_REQUIRE_NATS_SERVER=1
touch gate-job-start
cargo clippy --workspace --all-targets --locked -- -D warnings
code=0; cargo test --workspace --all-targets --locked --no-fail-fast || code=$?
if [ "$code" != 0 ]; then
  dump=""
  for _ in $(seq 1 12); do
    dump=$(find /c/CrashDumps -name "*.dmp" -newer gate-job-start 2>/dev/null | head -n 1 || true)
    [ -n "$dump" ] && break
    sleep 5
  done
  if [ -n "$dump" ]; then cp "$dump" gate-windows-crash.dmp; echo "crash dump: $dump"; else echo "crash dump: none written within 60 s"; fi
fi
exit "$code"'
windows_dump="$PWD/gate-windows-crash.dmp"
rm -f "$windows_dump"

step "Linux and Windows suites (remote, in parallel)"
ck motor run --vcpus 8 -- "set -euo pipefail
$linux_cmd" >"$logs/linux.log" 2>&1 &
linux_pid=$!
ck motor run --platform windows --return-path "$windows_dump" --return-max-bytes 134217728 -- "set -euo pipefail
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
if [ -f "$windows_dump" ]; then echo "windows: crash dump returned to $windows_dump"; fi

if [ "$status" = 0 ]; then
  echo "ASTROCYTE_GATE_OK $(git rev-parse HEAD)"
fi
exit "$status"
