#!/usr/bin/env bash
# Require green push CI on master or a train/* branch for the tagged commit.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ "$#" -ne 1 ]; then
    echo 'usage: scripts/release-ci-gate.sh <commit-ish>' >&2
    exit 2
fi
sha=$(git rev-parse --verify "$1^{commit}") || { echo 'release-ci-gate: REFUSED: cannot resolve commit' >&2; exit 2; }
git merge-base --is-ancestor "$sha" origin/master || {
    echo "release-ci-gate: REFUSED: $sha is not landed on origin/master (fetch master first)" >&2
    exit 2
}
exec python3 scripts/release/release_ci.py "$sha"
