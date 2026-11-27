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
gate_python=python3
if [ -x target/astrocyte-gate-python/bin/python ]; then
  gate_python=target/astrocyte-gate-python/bin/python
fi
"$gate_python" scripts/check_train_preconditions.py
step "overview doc references"
python3 scripts/check_doc_references.py
step "nothing internal tracked under .cortexkit"
scripts/check-cortexkit-layout.sh
step "no path dependency outside the repo"
python3 scripts/check-path-deps.py
step "hand-written Rust file sizes"
python3 scripts/check-file-sizes.py --self-test
python3 scripts/check-file-sizes.py
step "format"
scripts/fmt-check.sh

if [ "$fast_only" = 1 ]; then
  echo "ASTROCYTE_GATE_FAST_OK $(git rev-parse HEAD)"
  exit 0
fi

logs=$(mktemp -d "${TMPDIR:-/tmp}/astrocyte-gate.XXXXXX")
# Use the workflow's exact NATS release on the remote lanes. Neither a
# download nor a NATS installation belongs in the fast gate.
nats_pins=$("$gate_python" -c '
import pathlib, yaml
env = yaml.safe_load(pathlib.Path(".github/workflows/ci.yml").read_text())["env"]
print(env["NATS_SERVER_VERSION"], env["NATS_SERVER_SHA256_LINUX_AMD64"])
')
read -r nats_version nats_linux_sha <<< "$nats_pins"
# Linux jobs may use the network, so that lane installs the pinned archive.
linux_cmd='nats_dir=$(mktemp -d)
trap '\''rm -rf "$nats_dir"'\'' EXIT
bash scripts/lib/install-pinned-nats.sh "$NATS_TARGET" "$nats_dir"
export PATH="$nats_dir:$PATH" ASTROCYTE_REQUIRE_NATS_SERVER=1
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked'
# Windows build jobs have no network, so that lane uses the server on the
# build image and refuses to run if its version differs from the pin.
# A native crash in a test's child process (an access violation) leaves no
# stderr, only a minidump that Windows Error Reporting writes to C:\CrashDumps a
# few seconds after the process dies. When the suite fails, the job waits up to
# 60 s for a dump written during this job and copies it to the checkout root to
# be returned, so the faulting module can be read afterwards. In a Windows job
# `target/` links out of the checkout, and a returned file must sit inside it.
windows_cmd='nats-server --version | grep -q "v$NATS_SERVER_VERSION\b" || {
  echo "nats-server on the build image is not v$NATS_SERVER_VERSION: $(nats-server --version)"; exit 1; }
export ASTROCYTE_REQUIRE_NATS_SERVER=1
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
# `ck motor run` forwards no environment, so each lane sets the pins inside
# its own command.
ck motor run --vcpus 8 --net -- "set -euo pipefail
export NATS_SERVER_VERSION=$nats_version NATS_SERVER_SHA256=$nats_linux_sha NATS_TARGET=linux-amd64
$linux_cmd" >"$logs/linux.log" 2>&1 &
linux_pid=$!
ck motor run --platform windows \
  --return-path "$windows_dump" --return-max-bytes 134217728 -- "set -euo pipefail
export NATS_SERVER_VERSION=$nats_version
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
