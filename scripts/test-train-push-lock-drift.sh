#!/usr/bin/env bash
# Exercise the preflight against a local bare remote. No GitHub calls are needed.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
mkdir -p "$root/scratch"
scratch="$(mktemp -d "$root/scratch/train-push-lock.XXXXXX")"
trap 'cd "$root" && rm -rf "$scratch"' EXIT

unset GIT_DIR GIT_WORK_TREE GIT_COMMON_DIR GIT_INDEX_FILE GIT_CONFIG_COUNT GIT_CONFIG_PARAMETERS
unset TRAIN_PUSH_EXEC_COPY
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_ALLOW_PROTOCOL=file
export TMPDIR="$scratch" PATH="$scratch/bin:$PATH"
mkdir -p "$scratch/bin" "$scratch/repo/.github/workflows" "$scratch/repo/scripts/lib" \
  "$scratch/repo/member/src" "$scratch/sibling/src"
printf '#!/usr/bin/env bash\necho "unexpected GitHub call" >&2\nexit 1\n' > "$scratch/bin/gh"
chmod +x "$scratch/bin/gh"

cd "$scratch/repo"
cp "$root/scripts/train-push.sh" scripts/
cp "$root/scripts/lib/"{operator-gh.sh,workflow-gates.py} scripts/lib/
cat > .github/workflows/ci.yml <<'EOF'
on:
  push:
    branches: [master, 'train/**']
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo test
EOF
cat > Cargo.toml <<'EOF'
[workspace]
members = ["member"]
resolver = "2"
EOF
cat > member/Cargo.toml <<'EOF'
[package]
name = "workspace-member"
version = "0.1.0"
edition = "2021"

[dependencies]
sibling = { path = "../../sibling" }
EOF
cat > "$scratch/sibling/Cargo.toml" <<'EOF'
[package]
name = "sibling"
version = "0.3.0"
edition = "2021"
EOF
touch member/src/lib.rs "$scratch/sibling/src/lib.rs"
cat > Cargo.lock <<'EOF'
version = 4

[[package]]
name = "sibling"
version = "0.3.0"

[[package]]
name = "workspace-member"
version = "0.1.0"
dependencies = [
 "sibling",
]
EOF

git init -q -b master
git add .
git -c user.name='Preflight Test' -c user.email=test@example.invalid commit -qm setup
git clone -q --bare --no-local . "$scratch/origin.git"
git remote add origin "$scratch/origin.git"
git remote set-url --push origin "$scratch/no-push"
git symbolic-ref refs/remotes/origin/HEAD refs/remotes/origin/master
cp Cargo.lock "$scratch/committed.lock"

run_preflight() {
  REPO=test/preflight WATCH_CI_WORKFLOW=ci.yml bash scripts/train-push.sh lock-drift
}

workspace_member_lock_edit_is_preserved() {
  local output rc=0
  sed 's/"0\.1\.0"/"0.2.0"/' Cargo.lock > "$scratch/edited.lock"
  cp "$scratch/edited.lock" Cargo.lock
  output="$(run_preflight 2>&1)" || rc=$?
  if ! cmp -s Cargo.lock "$scratch/edited.lock"; then
    printf 'FAIL %s: preflight discarded the unstaged Cargo.lock edit\n%s\n' "${FUNCNAME[0]}" "$output" >&2
    return 1
  fi
  if [ "$rc" -ne 2 ] || [[ "$output" != *'working tree is not clean'* ||
    "$output" != *'workspace member workspace-member has a version edit'* ]]; then
    printf 'FAIL %s: expected dirty-tree refusal (got exit %s)\n%s\n' "${FUNCNAME[0]}" "$rc" "$output" >&2
    return 1
  fi
  printf 'PASS %s\n' "${FUNCNAME[0]}"
}

sibling_lock_drift_is_restored() {
  local output rc=0
  sed 's/"0\.3\.0"/"0.4.0"/' "$scratch/sibling/Cargo.toml" > "$scratch/sibling.toml"
  cp "$scratch/sibling.toml" "$scratch/sibling/Cargo.toml"
  sed 's/"0\.3\.0"/"0.4.0"/' "$scratch/committed.lock" > "$scratch/sibling-drift.lock"
  cp "$scratch/sibling-drift.lock" Cargo.lock
  output="$(run_preflight 2>&1)" || rc=$?
  # Deliberately omit check-path-deps.py so the train stops after clean-tree preflight.
  if ! cmp -s Cargo.lock "$scratch/committed.lock" || [ "$rc" -ne 2 ] ||
    [[ "$output" != *'restored Cargo.lock: sibling path-dependency drift in sibling'* ||
       "$output" != *'scripts/check-path-deps.py not found'* ]]; then
    printf 'FAIL %s: expected restoration and the next preflight (got exit %s)\n%s\n' "${FUNCNAME[0]}" "$rc" "$output" >&2
    return 1
  fi
  printf 'PASS %s\n' "${FUNCNAME[0]}"
}

metadata_failure_preserves_lock_edit() {
  local output rc=0
  printf '#!/usr/bin/env bash\nexit 127\n' > "$scratch/bin/cargo"
  chmod +x "$scratch/bin/cargo"
  cp "$scratch/sibling-drift.lock" Cargo.lock
  output="$(run_preflight 2>&1)" || rc=$?
  if ! cmp -s Cargo.lock "$scratch/sibling-drift.lock" || [ "$rc" -ne 2 ] ||
    [[ "$output" != *'could not determine workspace membership for sibling'* ||
       "$output" != *'working tree is not clean'* ]]; then
    printf 'FAIL %s: expected dirty-tree refusal without metadata (got exit %s)\n%s\n' "${FUNCNAME[0]}" "$rc" "$output" >&2
    return 1
  fi
  printf 'PASS %s\n' "${FUNCNAME[0]}"
}

workspace_member_lock_edit_is_preserved
sibling_lock_drift_is_restored
metadata_failure_preserves_lock_edit
