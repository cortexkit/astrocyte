#!/usr/bin/env python3
"""Require a green CI push run with every required job on the tagged commit."""

import json
import os
from pathlib import Path
import subprocess
import sys


def gh_json(*args):
    try:
        return json.loads(subprocess.check_output(["gh", *args], text=True))
    except (subprocess.CalledProcessError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot query GitHub: {' '.join(args)}") from error


def eligible(run, sha):
    branch = run.get("headBranch", "")
    return (
        run.get("headSha") == sha
        and run.get("event") == "push"
        and (branch == "master" or branch.startswith("train/"))
        and run.get("status") == "completed"
        and run.get("conclusion") == "success"
    )


def required_checks(protection, listed):
    names = {line.split("#", 1)[0].strip() for line in listed.splitlines()}
    names.discard("")
    if protection is not None:
        protected = set(protection.get("contexts", []))
        protected.update(check["context"] for check in protection.get("checks", []))
        if names != protected:
            print("release-ci-gate: required-check list differs from protection; enforcing their union")
        names.update(protected)
    if not names:
        raise ValueError("no required checks known; protection and committed list are empty/unavailable")
    return names


def missing_checks(jobs, required):
    bad = []
    for name in sorted(required):
        matches = [job for job in jobs if job.get("name") == name]
        if not matches:
            bad.append(f"{name}: missing")
        elif any(job.get("status") != "completed" or job.get("conclusion") != "success" for job in matches):
            bad.append(f"{name}: not every matching job completed successfully")
    return bad


def main(sha):
    repo = os.environ.get("GITHUB_REPOSITORY", "cortexkit/astrocyte")
    listed = Path(__file__).with_name("required-checks.txt").read_text()
    try:
        protection = gh_json("api", f"repos/{repo}/branches/master/protection/required_status_checks")
    except ValueError:
        protection = None
        print("release-ci-gate: protection unreadable; using scripts/release/required-checks.txt")
    required = required_checks(protection, listed)
    runs = gh_json(
        "run", "list", "-R", repo, "--workflow", "ci.yml", "--commit", sha, "--limit", "100",
        "--json", "databaseId,headSha,headBranch,event,status,conclusion",
    )
    candidates = sorted((run for run in runs if eligible(run, sha)), key=lambda run: run["databaseId"], reverse=True)
    if not candidates:
        raise ValueError(f"no successful ci.yml push run on master/train for exact commit {sha}")
    problems = []
    for run in candidates:
        run_id = str(run["databaseId"])
        # Read the run back with its jobs so a rerun cannot mix an older green
        # conclusion with a newer attempt's incomplete or skipped checks.
        details = gh_json(
            "run", "view", run_id, "-R", repo, "--json",
            "headSha,headBranch,event,status,conclusion,jobs",
        )
        if not eligible(details, sha):
            problems.append(f"run {run_id}: no longer successful/eligible")
            continue
        bad = missing_checks(details["jobs"], required)
        if not bad:
            print(f"release-ci-gate: OK {sha}, run {run_id}, {len(required)}/{len(required)} required checks green")
            return
        problems.append(f"run {run_id}: {'; '.join(bad)}")
    raise ValueError("no eligible run has every required check green: " + " | ".join(problems))


if __name__ == "__main__":
    try:
        main(sys.argv[1])
    except (ValueError, KeyError, OSError, TypeError) as error:
        print(f"release-ci-gate: REFUSED: {error}", file=sys.stderr)
        sys.exit(2)
