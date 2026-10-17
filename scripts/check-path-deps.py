#!/usr/bin/env python3
"""Refuse any dependency that resolves to a path outside this repository.

A path dependency on a sibling checkout (`path = "../subconscious/..."`)
builds against whatever that checkout holds, so `Cargo.lock` goes stale the
moment the sibling moves and the `--locked` gate fails for a reason this repo
did not cause. Fleet rule: sibling crates come from crates.io at a pinned
version, and only this repo's own crates are path dependencies.

Reads `cargo metadata` and fails on either of two things:
  - a dependency declared with a `path` that resolves outside the repo root;
  - a resolved package with no registry or git source whose manifest sits
    outside the repo root (this is how a `[patch]` entry pointing at a sibling
    checkout shows up).

Before reading the real workspace, the check runs against a clean sample and
two planted violations, one per rule, and exits 1 unless the clean sample
passes and both violations are caught. Without that, a check edited into one
that can never fail would still print a clean line.

Exit 1 on any finding, naming it. Run unpiped: a pipeline reports the last
stage's status.
"""

import json
import os
import subprocess
import sys

ROOT = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))


def inside(path: str, root: str) -> bool:
    real = os.path.realpath(path)
    return real == root or real.startswith(root + os.sep)


def check(meta: dict, root: str) -> list:
    """Every path that leaves `root`, as readable findings."""
    findings = []
    for package in meta.get("packages", []):
        for dep in package.get("dependencies", []):
            dep_path = dep.get("path")
            if dep_path is not None and not inside(dep_path, root):
                findings.append(
                    f"{package['name']} depends on {dep['name']} by path {dep_path}"
                )
        if package.get("source") is None and not inside(package["manifest_path"], root):
            findings.append(
                f"package {package['name']} {package.get('version', '?')} resolves to "
                f"{package['manifest_path']}, outside the repo (a path or [patch] entry)"
            )
    return findings


def sample(root: str) -> dict:
    """A clean workspace: one member, one in-repo path dep, one registry dep."""
    return {
        "packages": [
            {
                "name": "member",
                "version": "0.1.0",
                "source": None,
                "manifest_path": os.path.join(root, "crates/member/Cargo.toml"),
                "dependencies": [
                    {"name": "sibling-in-repo", "path": os.path.join(root, "crates/other")},
                    {"name": "subc-protocol"},
                ],
            },
            {
                "name": "subc-protocol",
                "version": "0.29.1",
                "source": "registry+https://github.com/rust-lang/crates.io-index",
                "manifest_path": "/registry/subc-protocol-0.29.1/Cargo.toml",
                "dependencies": [],
            },
        ]
    }


def controls() -> list:
    """Why the check can't be trusted on this run, or nothing."""
    root = "/planted/repo"
    broken = []
    clean = sample(root)
    if check(clean, root):
        broken.append(f"the clean sample fails: {check(clean, root)!r}")

    declared = sample(root)
    declared["packages"][0]["dependencies"].append(
        {"name": "subc-os", "path": "/planted/subconscious/crates/subc-os"}
    )
    if not check(declared, root):
        broken.append("planted violation not caught: a declared path dependency outside the repo")

    patched = sample(root)
    patched["packages"][1]["source"] = None
    patched["packages"][1]["manifest_path"] = "/planted/subconscious/crates/subc-protocol/Cargo.toml"
    if not check(patched, root):
        broken.append("planted violation not caught: a [patch]-resolved package outside the repo")
    return broken


def main() -> int:
    broken = controls()
    for reason in broken:
        print(f"CONTROL {reason}")
    if broken:
        return 1
    result = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--locked"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        # Any refusal by `cargo metadata --locked` lands here, including a new
        # path dependency committed without its Cargo.lock change. Print
        # cargo's own message so the reason is visible, not a traceback.
        print("FAIL cargo metadata --locked refused the workspace:")
        print(result.stderr.strip())
        return 1
    meta = json.loads(result.stdout)
    findings = check(meta, ROOT)
    path_packages = sum(1 for p in meta["packages"] if p.get("source") is None)
    if path_packages == 0:
        # Every workspace member is itself a path package, so a correct read
        # always sees at least one. Zero means the read saw nothing, and a
        # scan of nothing must not pass as a clean result.
        print("FAIL cargo metadata listed no path packages; the check read nothing")
        return 1
    print(
        f"path dependencies: {path_packages} path packages, all inside the repo"
        if not findings
        else f"path dependencies: {len(findings)} outside the repo"
    )
    print("controls: 2 planted violations caught")
    for finding in findings:
        print(f"FAIL {finding}")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
