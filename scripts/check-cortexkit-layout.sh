#!/usr/bin/env bash
# Nothing under .cortexkit/ may be tracked except the allowed folders and files.
#
# The fleet layout keeps internal material (design notes, investigations, tool state)
# under .cortexkit/, ignored by git. It tracks only release-notes/, skills/, rules/,
# WORKER.md, and top-level project settings named <module>.jsonc there. Check every
# tracked path against that allowlist: ignore rules do not catch a private path that a
# later negation makes visible.
#
# That check passes vacuously if the ignore block is missing, because then nothing is
# ignored. So it first confirms the block works: an internal path must be ignored and
# an allowed one must not be. If either control fails, the checker reports itself
# BLIND and exits 2 rather than passing.
#
# Exit 0 clean, 1 a disallowed file is tracked, 2 the controls or git listing fail.

set -u
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 2

if ! git check-ignore -q --no-index .cortexkit/internal-docs/designs/control.md; then
  echo "BLIND: .cortexkit/internal-docs/ is not ignored; the .cortexkit block is missing from .gitignore" >&2
  exit 2
fi
if git check-ignore -q --no-index .cortexkit/release-notes/control.md; then
  echo "BLIND: .cortexkit/release-notes/ is ignored; the block's allow lines are missing" >&2
  exit 2
fi

if ! tracked="$(git ls-files -- .cortexkit)"; then
  echo "could not list tracked paths under .cortexkit/" >&2
  exit 2
fi

disallowed=()
while IFS= read -r path; do
  [ -n "$path" ] || continue
  case "$path" in
    .cortexkit/release-notes/*|.cortexkit/skills/*|.cortexkit/rules/*|.cortexkit/WORKER.md)
      continue
      ;;
  esac
  if [[ "$path" =~ ^\.cortexkit/[a-z0-9-]+\.jsonc$ ]]; then
    continue
  fi
  disallowed+=("$path")
done <<< "$tracked"

if [ "${#disallowed[@]}" -gt 0 ]; then
  echo "tracked files under .cortexkit/ that the layout keeps out of git:" >&2
  printf '  %s\n' "${disallowed[@]}" >&2
  echo "move them to .cortexkit/internal-docs/ (or remove them) and 'git rm --cached' them" >&2
  exit 1
fi
echo "controls hold; no disallowed file is tracked under .cortexkit/"
