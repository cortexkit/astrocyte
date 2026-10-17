#!/usr/bin/env bash
# Sourced by scripts/train-push.sh as a repo-local preflight. It may call
# `say` and `refuse`; everything here is a NOTICE and never refuses, because a
# scheduled red is about what already landed and the push in front of it may be
# the fix.
#
# WHY THIS EXISTS AT ALL. The daily workflow checks an unchanged commit with the
# current stable compiler and runner image. A failure can therefore expose a
# toolchain or runner regression that a new push cannot distinguish from its own
# changes. This preflight puts that result in front of the next train operator.
#
# WHY THE NEWEST RUN IS NOT THE ONE TO GRADE. Pushes and scheduled runs use
# separate concurrency groups; only superseded pushes to the same train ref are
# cancelled automatically. A cancelled sample has no verdict, and an older
# completed sample may still be the newest result worth grading.

# A cancellation is not a failure. No scheduled sample was taken, so nothing
# broke; the previous completed sample remains the latest measured outcome.
# Report that separately from a failed run, which has a verdict and needs attention.
#
# --event schedule is filtered SERVER-SIDE, so the limit applies to the filtered
# set and a burst of pushes cannot empty the window. A bare --limit is a count
# masquerading as a time window, and it narrows precisely when there is most to
# look at.
#
# Keep the API request's exit status separate from its output. An unreachable
# API can return no rows just like a schedule that has never run, so treating
# both as the same empty result would report a false missing-schedule notice.
#
# The check distinguishes three outcomes: no scheduled run, a query that could
# not be completed, or one or more scheduled runs. The CLI can return empty text
# for both a successful empty result and a request failure, so its exit status
# must be captured before deciding that no run exists. Grading uses the returned
# run text directly; Python only computes the age notice, so missing Python does
# not hide a failed scheduled run.

# Under `set -e`, a bare `VAR=$(failing)` exits the caller before it can read
# the command status. Putting the assignment in an `||` list preserves that
# status and lets the preflight report an unavailable query without aborting.
_sched_rc=0
# Use the separately installed GitHub CLI when present to bypass any PATH
# wrapper, whose cached runs page can be stale. Fall back to PATH when no
# standalone CLI is installed.
_sched_gh=/opt/zerobrew/bin/gh
[ -x "$_sched_gh" ] || _sched_gh=gh
# Sorted here rather than trusting the API's order, so the newest run is read
# by time whatever order a page arrives in.
_sched_window=$("$_sched_gh" api \
  'repos/cortexkit/astrocyte/actions/runs?event=schedule&per_page=10' \
  --jq '.workflow_runs | sort_by(.created_at) | reverse | .[] | "\(.conclusion // .status) \(.created_at[0:16]) \(.head_sha[0:8])"' \
  2>/dev/null) || _sched_rc=$?

if [ "$_sched_rc" -ne 0 ]; then
  say "NOTICE: the scheduled-run check DID NOT RUN (gh exited ${_sched_rc})."
  say "  This is not a finding about the cron: nothing was asked and nothing is known."
elif [ -z "$_sched_window" ]; then
  # Empty covers two different states and only one is broken, so say which this
  # is: a cron that never fired and a cron that was removed look identical from
  # the runs API alone.
  _repo_root=$(git rev-parse --show-toplevel)
  _cron_declared=$(grep -l 'cron:' "$_repo_root"/.github/workflows/*.yml 2>/dev/null | head -1 || true)
  if [ -n "$_cron_declared" ]; then
    say "NOTICE: a cron is declared in $(basename "$_cron_declared") but NO scheduled run exists."
    say "  A schedule that never fires looks exactly like one that passes."
  fi
else
  # Liveness and grading want different populations. A cancelled run still proves
  # the scheduler FIRED, so the age arm takes the newest run whatever its
  # conclusion; only grading skips cancellations.
  _newest_at=$(printf '%s\n' "$_sched_window" | head -1 | cut -d' ' -f2)
  _age_h=$(python3 - "$_newest_at" <<'PY' 2>/dev/null || true
import datetime, sys
stamp = sys.argv[1]
if stamp:
    when = datetime.datetime.fromisoformat(stamp + ":00+00:00")
    now = datetime.datetime.now(datetime.timezone.utc)
    print(int((now - when).total_seconds() // 3600))
PY
)
  # 48h, not 24h: this cron is declared for 06:00Z and lands nearer 10:30Z on
  # GitHub's shared queue, and the lag has been four hours or more every time.
  # An alarm that fires on healthy work gets muted, and a muted alarm is how the
  # next real one reaches nobody.
  if [ -n "${_age_h:-}" ] && [ "$_age_h" -gt 48 ]; then
    say "NOTICE: the last SCHEDULED run is ${_age_h}h old."
    say "  A cron that stopped firing leaves its last conclusion behind and reads healthy."
    # This notice has fired on pages that were days stale while the same query
    # answered fresh a moment later. Record what answered, so the next stale
    # page can be traced: which gh, GitHub's request id and date, any cache age,
    # and what a second identical read says right now.
    _sched_headers=$("$_sched_gh" api -i \
      'repos/cortexkit/astrocyte/actions/runs?event=schedule&per_page=10' 2>/dev/null \
      | grep -iE '^(date|age|x-github-request-id|cache-control|etag):' || true)
    _sched_reread=$("$_sched_gh" api \
      'repos/cortexkit/astrocyte/actions/runs?event=schedule&per_page=10' \
      --jq '[.workflow_runs[].created_at] | max' 2>/dev/null || true)
    say "  diagnostics: gh=$_sched_gh first-read newest=$_newest_at re-read newest=${_sched_reread:-?}"
    if [ -n "$_sched_headers" ]; then
      printf '%s\n' "$_sched_headers" | while IFS= read -r _h; do [ -n "$_h" ] && say "    $_h"; done
    fi
  fi

  _gradable=$(printf '%s\n' "$_sched_window" | grep -v '^cancelled ' | head -1 || true)
  if [ -z "$_gradable" ]; then
    _cancelled_count=$(printf '%s\n' "$_sched_window" | grep -c '^cancelled ' || true)
    say "NOTICE: all ${_cancelled_count} scheduled runs in the window were CANCELLED — samples never taken."
    say "  Nothing is known to be broken and nothing was measured."
  else
    case "$_gradable" in
      success*) : ;;
      *)
        say "NOTICE: the newest non-cancelled SCHEDULED run was not green:"
        say "  $_gradable"
        say "  It builds an unchanged sha on the current toolchain, so its red is not your diff."
        say "  gh api 'repos/cortexkit/astrocyte/actions/runs?event=schedule&per_page=10'"
        ;;
    esac
  fi
fi

unset _sched_window _sched_rc _newest_at _age_h _gradable _cancelled_count _cron_declared _repo_root
