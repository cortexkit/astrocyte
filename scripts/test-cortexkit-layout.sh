#!/usr/bin/env bash
# Exercise the .cortexkit layout checker in isolated repositories.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
mkdir -p "$root/scratch"
scratch="$(mktemp -d "$root/scratch/cortexkit-layout.XXXXXX")"
trap 'cd "$root" && rm -rf "$scratch"' EXIT

unset GIT_DIR GIT_WORK_TREE GIT_COMMON_DIR GIT_INDEX_FILE GIT_CONFIG_COUNT GIT_CONFIG_PARAMETERS
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null
real_git="$(command -v git)"

make_repo() {
  local name="$1"
  repo="$scratch/$name"
  mkdir -p "$repo/scripts" "$repo/.cortexkit/release-notes" \
    "$repo/.cortexkit/skills" "$repo/.cortexkit/rules"
  cp "$root/scripts/check-cortexkit-layout.sh" "$repo/scripts/"
  cat > "$repo/.gitignore" <<'EOF'
.cortexkit/*
!.cortexkit/release-notes/
!.cortexkit/skills/
!.cortexkit/rules/
!.cortexkit/WORKER.md
!.cortexkit/private.txt
EOF
  cd "$repo"
  git init -q
}

run_checker() {
  local rc=0
  output="$(bash scripts/check-cortexkit-layout.sh 2>&1)" || rc=$?
  actual_rc="$rc"
}

tracked_unignored_private_file_is_refused() {
  make_repo private
  printf 'private\n' > .cortexkit/private.txt
  git add .gitignore .cortexkit/private.txt
  run_checker
  if [ "$actual_rc" -ne 1 ] || [[ "$output" != *'.cortexkit/private.txt'* ]]; then
    printf 'FAIL %s: expected exit 1 naming the private path (got %s)\n%s\n' \
      "${FUNCNAME[0]}" "$actual_rc" "$output" >&2
    return 1
  fi
  printf 'PASS %s\n' "${FUNCNAME[0]}"
}

allowed_files_pass() {
  make_repo allowed
  printf 'release\n' > .cortexkit/release-notes/release.md
  printf 'skill\n' > .cortexkit/skills/skill.md
  printf 'rule\n' > .cortexkit/rules/rule.md
  printf 'worker\n' > .cortexkit/WORKER.md
  git add .gitignore .cortexkit/release-notes/release.md .cortexkit/skills/skill.md \
    .cortexkit/rules/rule.md .cortexkit/WORKER.md
  run_checker
  if [ "$actual_rc" -ne 0 ]; then
    printf 'FAIL %s: expected allowed files to pass (got %s)\n%s\n' \
      "${FUNCNAME[0]}" "$actual_rc" "$output" >&2
    return 1
  fi
  printf 'PASS %s\n' "${FUNCNAME[0]}"
}

project_settings_file_passes() {
  make_repo settings
  printf '{}\n' > .cortexkit/astrocyte.jsonc
  git add .gitignore
  git add -f .cortexkit/astrocyte.jsonc
  run_checker
  if [ "$actual_rc" -ne 0 ]; then
    printf 'FAIL %s: expected project settings to pass (got %s)\n%s\n' \
      "${FUNCNAME[0]}" "$actual_rc" "$output" >&2
    return 1
  fi
  printf 'PASS %s\n' "${FUNCNAME[0]}"
}

nested_jsonc_is_refused() {
  make_repo nested-jsonc
  mkdir -p .cortexkit/x
  printf '{}\n' > .cortexkit/x/y.jsonc
  git add .gitignore
  git add -f .cortexkit/x/y.jsonc
  run_checker
  if [ "$actual_rc" -ne 1 ] || [[ "$output" != *'.cortexkit/x/y.jsonc'* ]]; then
    printf 'FAIL %s: expected exit 1 naming the nested JSONC path (got %s)\n%s\n' \
      "${FUNCNAME[0]}" "$actual_rc" "$output" >&2
    return 1
  fi
  printf 'PASS %s\n' "${FUNCNAME[0]}"
}

git_listing_failure_is_not_clean() {
  make_repo git-failure
  mkdir -p "$scratch/bin"
  printf '#!/usr/bin/env bash\nif [ "$1" = ls-files ]; then exit 75; fi\nexec %q "$@"\n' \
    "$real_git" > "$scratch/bin/git"
  chmod +x "$scratch/bin/git"
  PATH="$scratch/bin:$PATH" run_checker
  if [ "$actual_rc" -ne 2 ]; then
    printf 'FAIL %s: expected git failure to exit 2 (got %s)\n%s\n' \
      "${FUNCNAME[0]}" "$actual_rc" "$output" >&2
    return 1
  fi
  printf 'PASS %s\n' "${FUNCNAME[0]}"
}

status=0
tracked_unignored_private_file_is_refused || status=1
allowed_files_pass || status=1
project_settings_file_passes || status=1
nested_jsonc_is_refused || status=1
git_listing_failure_is_not_clean || status=1
exit "$status"
