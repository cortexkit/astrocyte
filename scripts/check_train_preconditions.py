#!/usr/bin/env python3
"""Assert the CI workflow still satisfies the train-branch preconditions.

The train shape lands a sha on master only after CI on a train/** push is
green. Four properties of ci.yml make that true, each verified once when
the shape was adopted and then trusted while the file keeps being edited:

  1. the push trigger includes train/**   -- else a train push produces no
     check at all, and enabling protection later makes master unpushable;
  2. no job or step `if:` keys on github.ref or github.event_name  -- else
     the workflow fires on a train and the gate it carries silently skips;
  3. concurrency.group keys on github.ref  -- else a train push cancels the
     in-flight master run sharing the group;
  4. the default branch stays in the push trigger  -- while this repo is
     unprotected, that trigger is the only observer of an off-train push, so a
     workflow that quietly became train-only has lost its last observer and a
     checker asserting only (1) would pass it.

Each `if:` VALUE is read from the parsed document, never grepped: a folded
`if: >-` puts its condition on the lines below the key, and a line scan
returns clean on exactly the workflow this exists to catch.

Exit 1 on any drift, naming it. Run unpiped: a pipeline reports the last
stage's status, and three named failures behind `| head` read as exit 0.

Before reading ci.yml, the checks run against planted workflows, one per
property, and the script exits 1 unless each planted drift is caught and a
clean workflow passes. Without that, a check edited into one that can never
fail would still print a clean line.
"""

from __future__ import annotations

import pathlib
import sys

import yaml

WORKFLOW = pathlib.Path(__file__).resolve().parent.parent / ".github" / "workflows" / "ci.yml"
TRAIN_GLOB = "train/**"
DEFAULT_BRANCH = "master"
PATH_TOKENS = ("github.ref", "github.event_name")


def check(doc: dict) -> tuple[list[str], list, int, str]:
    """The drift found in one parsed workflow, with what was read."""
    # PyYAML parses the bare key `on` as boolean True.
    triggers = doc.get(True, doc.get("on", {})) or {}
    failures: list[str] = []

    branches = ((triggers.get("push") or {}).get("branches")) or []
    if TRAIN_GLOB not in branches:
        failures.append(f"push trigger lacks {TRAIN_GLOB!r}: branches={branches!r}")
    if DEFAULT_BRANCH not in branches:
        failures.append(
            f"push trigger lacks the default branch {DEFAULT_BRANCH!r}: branches={branches!r} "
            "(unprotected repo: this trigger is the only observer of an off-train push)"
        )

    conditions = 0
    for job_name, job in (doc.get("jobs") or {}).items():
        sites = [("job", job.get("if"))]
        sites += [(f"step {s.get('name') or s.get('uses') or '?'}", s.get("if")) for s in job.get("steps") or []]
        for site, value in sites:
            if value is None:
                continue
            conditions += 1
            text = str(value)
            if any(token in text for token in PATH_TOKENS):
                failures.append(f"path-dependent condition in job {job_name!r} at {site}: {text.strip()!r}")

    group = str((doc.get("concurrency") or {}).get("group", ""))
    if "github.ref" not in group:
        failures.append(f"concurrency.group does not key on github.ref: {group!r}")
    return failures, branches, conditions, group


# A clean workflow, and one drift per property. The folded `if: >-` is the
# shape a line scan misses, so it is the one planted for property 2.
CLEAN = """
on:
  push:
    branches: [master, 'train/**']
concurrency:
  group: ci-${{ github.ref }}
jobs:
  test:
    steps:
      - name: Test
        if: success()
        run: cargo test
"""
PLANTED = {
    "no train trigger": CLEAN.replace(", 'train/**'", ""),
    "no default branch": CLEAN.replace("master, ", ""),
    "path-dependent if": CLEAN.replace(
        "if: success()", "if: >-\n          github.ref == 'refs/heads/master'"
    ),
    "group off the ref": CLEAN.replace("ci-${{ github.ref }}", "ci-shared"),
}


def controls() -> list[str]:
    """Why the checks can't be trusted on this run, or nothing."""
    broken = []
    if check(yaml.safe_load(CLEAN))[0]:
        broken.append(f"the clean control workflow fails: {check(yaml.safe_load(CLEAN))[0]!r}")
    for name, text in PLANTED.items():
        if not check(yaml.safe_load(text))[0]:
            broken.append(f"planted drift not caught: {name}")
    return broken


def main() -> int:
    broken = controls()
    for reason in broken:
        print(f"CONTROL {reason}")
    if broken:
        return 1
    failures, branches, conditions, group = check(yaml.safe_load(WORKFLOW.read_text()))
    print(
        f"train preconditions: branches={branches!r} if-keys={conditions} group={group!r} "
        f"controls={len(PLANTED)} caught"
    )
    for failure in failures:
        print(f"FAIL {failure}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
