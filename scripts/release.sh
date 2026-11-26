#!/usr/bin/env bash
# Stage a release draft only from a clean, landed master; never publish it.
set -euo pipefail

finished=0
scratch=""
tag=""
tag_state="not created"
landed="not landed"
workflow="not started"
draft_state="not confirmed"
repo="cortexkit/astrocyte"

refuse() { printf 'release: REFUSED: %s\n' "$*" >&2; exit 2; }
on_exit() {
    local rc=$?
    trap - EXIT
    if [ "$finished" != 1 ]; then
        printf 'release: REFUSED: release did not complete (exit %s)\n' "$rc" >&2
        printf 'release: stopped (exit %s); master: %s; tag %s: %s; workflow: %s; draft: %s\n' \
            "$rc" "$landed" "$tag" "$tag_state" "$workflow" "$draft_state" >&2
        if [ "$tag_state" != "not created" ]; then
            # A failed workflow can leave a partial draft behind. Query it so
            # the operator knows what to inspect before retrying.
            if ! "$python" "$helper" publication "$repo" "$tag" >&2; then
                echo 'release: GitHub release/assets could not be verified (possibly absent); inspect before retrying.' >&2
            fi
            echo 'release: no rollback performed; never move or replace a published tag.' >&2
        else
            echo 'release: no release tag or GitHub draft was created by this command.' >&2
        fi
        [ -z "$scratch" ] || rm -rf "$scratch"
        exit 2
    fi
    [ -z "$scratch" ] || rm -rf "$scratch"
    exit 0
}
trap on_exit EXIT
trap 'refuse "interrupted"' INT TERM

cd "$(dirname "$0")/.." || refuse "cannot reach repository root"
helper="$PWD/scripts/release/release.py"
version="${1:-}"
dry_run=0
if [ "$#" -eq 2 ] && [ "$2" = --dry-run ]; then
    dry_run=1
elif [ "$#" -ne 1 ]; then
    refuse "usage: scripts/release.sh X.Y.Z [--dry-run]"
fi
python=""
for candidate in python3 python3.14 python3.13 python3.12 python3.11; do
    if "$candidate" -c 'import tomllib' >/dev/null 2>&1; then
        python="$candidate"
        break
    fi
done
[ -n "$python" ] || refuse "Python 3.11+ is required"
"$python" "$helper" check-version "$version" </dev/null || refuse "invalid release version"
tag="v$version"
floor=$("$python" "$helper" floor release-manifest.json) || refuse "invalid release-manifest.json requires_core"
for tool in git cargo gh; do
    command -v "$tool" >/dev/null || refuse "missing command: $tool"
done
[ "$(git symbolic-ref --quiet --short HEAD)" = master ] || refuse "checkout must be on master"
status=$(git status --porcelain --untracked-files=all) || refuse "cannot inspect checkout status"
[ -z "$status" ] || refuse "checkout is dirty; commit the release notes first"
git fetch --prune origin '+refs/heads/master:refs/remotes/origin/master' --tags || refuse "fetch of origin/master and tags failed"
start_sha=$(git rev-parse HEAD)
[ "$start_sha" = "$(git rev-parse refs/remotes/origin/master)" ] || refuse "master is not equal to freshly fetched origin/master"
tags=$(git tag --list 'v*')
printf '%s\n' "$tags" | "$python" "$helper" check-version "$version" || refuse "version/tag check failed"
notes=".cortexkit/release-notes/$tag.md"
[ -s "$notes" ] || refuse "missing or empty $notes"
"$python" "$helper" check-crates "$version" || refuse "crate version bump is not valid"
[ "$(git config user.email)" = '94532+ualtinok@users.noreply.github.com' ] || refuse "git user.email is not the release identity"
gh auth status || refuse "GitHub CLI is not authenticated"

if [ "$dry_run" = 1 ]; then
    scripts/release-ci-gate.sh "$start_sha" || refuse "landed master has no exact-commit push CI"
    echo "release: preflight passed for $tag (declared requires_core $floor); no versions, commits, branches, tags or releases changed"
    finished=1
    exit 0
fi

mkdir -p target/scratch
scratch=$(mktemp -d "$PWD/target/scratch/release.XXXXXX")
manifests=(crates/astrocyte-core/Cargo.toml crates/astrocyte-module/Cargo.toml crates/astrocyte-readers/Cargo.toml)
cp Cargo.lock "$scratch/Cargo.lock.before"
"$python" "$helper" bump "$version" || refuse "crate version bump failed"
cargo update --workspace --offline || refuse "offline workspace lock refresh failed"
"$python" "$helper" check-lock "$scratch/Cargo.lock.before" Cargo.lock "$version" || refuse "lock refresh was not workspace-only"
[ "$(git rev-parse HEAD)" = "$start_sha" ] || refuse "HEAD changed during the version bump"

# Commit only the intended bump. Foreign edits must not hitch a ride on a release.
foreign=$(git status --porcelain --untracked-files=all -- . ':!Cargo.lock' "${manifests[@]/#/:!}")
[ -z "$foreign" ] || refuse "foreign changes appeared during release preparation: $foreign"
git diff --check || refuse "version bump has whitespace errors"
git add -- "${manifests[@]}" Cargo.lock || refuse "cannot stage version bump"
if ! git commit --only -F - -- "${manifests[@]}" Cargo.lock <<MSG
release: $tag
MSG
then
    refuse "cannot commit version bump"
fi
release_sha=$(git rev-parse HEAD)
WATCH_CI_WORKFLOW=ci.yml scripts/train-push.sh "release-$version" || refuse "release train failed; bump commit $release_sha remains local or on its train branch"
git fetch origin '+refs/heads/master:refs/remotes/origin/master' || refuse "cannot verify landed master"
[ "$(git rev-parse origin/master)" = "$release_sha" ] || refuse "origin/master is not the release commit $release_sha"
landed="$release_sha"
[ "$(git rev-parse HEAD)" = "$release_sha" ] && [ -z "$(git status --porcelain)" ] || refuse "checkout changed while train was running"
scripts/release-ci-gate.sh "$release_sha" || refuse "release commit has no exact-commit push CI"
git tag -a "$tag" "$release_sha" -m "astrocyte $version" || refuse "cannot create annotated tag $tag"
tag_state="local only; not pushed"
tag_state="push attempted; remote state unconfirmed"
git push origin "refs/tags/$tag" || refuse "tag push failed; remote state may be ambiguous"
tag_state="pushed to origin at $release_sha"

run=""
for attempt in 1 2 3 4 5 6; do
    echo "release: locating $tag workflow (attempt $attempt/6)"
    runs=$(gh run list -R "$repo" --workflow release.yml --commit "$release_sha" --event push \
        --limit 100 --json databaseId,headSha,headBranch,event --jq \
        "[.[] | select(.headSha == \"$release_sha\" and .headBranch == \"$tag\" and .event == \"push\")] | sort_by(.databaseId) | last | .databaseId") || refuse "cannot list release workflow runs after tag push"
    case "$runs" in
        ''|null|*[!0-9]*) ;;
        *) run="$runs"; break ;;
    esac
    sleep 10
done
[ -n "$run" ] || refuse "no release.yml push run appeared for $tag at $release_sha after 60 seconds"
workflow="run $run in progress"
# `gh run watch` can return early while the run is still going (a dropped or
# rate-limited request ends it with rc 1). Watch again until GitHub reports the
# run completed, so a slow macOS leg isn't read as a failed release.
conclusion=""
watch_deadline=$((SECONDS + 5400))
while [ "$SECONDS" -lt "$watch_deadline" ]; do
    gh run watch "$run" -R "$repo" --exit-status --interval 30 >/dev/null 2>&1 || true
    conclusion=$(gh run view "$run" -R "$repo" --json status,conclusion --jq '.status + "/" + .conclusion' 2>/dev/null) || conclusion=""
    case "$conclusion" in
        completed/*) break ;;
    esac
    sleep 30
done
[ -n "$conclusion" ] || refuse "cannot read release workflow conclusion"
workflow="run $run $conclusion"
[ "$conclusion" = completed/success ] || refuse "release workflow did not succeed ($workflow); tag is already pushed"

"$python" "$helper" verify-draft "$repo" "$tag" || refuse "workflow succeeded, but no complete draft was found"
draft_state="verified"
echo "release: staged $tag ($release_sha), requires_core $floor; review and publish the draft in GitHub when ready"
finished=1
