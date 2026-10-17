#!/usr/bin/env bash
# Format check over THIS workspace's packages only.
#
# `cargo fmt --all` also follows path dependencies outside the workspace, so
# this names the workspace's own packages instead. The package list is derived
# from `cargo metadata --no-deps` at run time rather than typed, and an empty
# list refuses: fmt over zero packages passes vacuously, which is the wrong
# direction for a gate.
set -euo pipefail

pkgs=$(cargo metadata --no-deps --format-version 1 \
  | python3 -c 'import json,sys; print(" ".join("-p " + p["name"] for p in json.load(sys.stdin)["packages"]))')

if [ -z "$pkgs" ]; then
  echo "fmt gate: cargo metadata --no-deps returned zero packages; refusing a vacuous pass" >&2
  exit 2
fi

echo "fmt gate: packages: $pkgs"
# shellcheck disable=SC2086
exec cargo fmt $pkgs -- --check "$@"
