#!/usr/bin/env python3
"""Small, offline validators used by the release driver."""

import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tomllib
import zipfile


CRATES = ("astrocyte-core", "astrocyte-module", "astrocyte-readers")
TARGETS = ("darwin-arm64", "linux-x64", "linux-arm64", "windows-x64", "windows-arm64")
ARCHIVES = {f"ck-astrocyte-{target}.zip" for target in TARGETS}
ASSETS = ARCHIVES | {f"{name}.sha256" for name in ARCHIVES} | {"release-manifest.json"}


def semver(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", value
    ):
        raise ValueError(f"expected X.Y.Z without leading zeroes, got {value!r}")
    return tuple(map(int, value.split(".")))


def check_version(version, tags):
    proposed = semver(version)
    released = []
    for tag in tags.splitlines():
        if tag == "v" + version:
            raise ValueError(f"tag {tag} already exists")
        try:
            released.append((semver(tag.removeprefix("v")), tag))
        except ValueError:
            continue
    if released and proposed <= max(released)[0]:
        raise ValueError(f"{version} must be greater than latest release {max(released)[1]}")


def core_floor(path):
    manifest = json.loads(Path(path).read_text())
    floor = manifest.get("requires_core") if isinstance(manifest, dict) else None
    try:
        semver(floor)
    except ValueError as error:
        raise ValueError(f"{path}: requires_core must be a verified MAJOR.MINOR.PATCH string") from error
    return floor


def crate_updates(version):
    semver(version)
    paths = [Path(f"crates/{name}/Cargo.toml") for name in CRATES]
    replacements = []
    for path in paths:
        text = path.read_text()
        old = tomllib.loads(text)["package"]["version"]
        semver(old)
        if semver(version) <= semver(old):
            raise ValueError(f"{path}: requested version must exceed {old}")
        updated, count = re.subn(
            r'(?m)^version = "' + re.escape(old) + r'"$', f'version = "{version}"', text
        )
        if count != 1:
            raise ValueError(f"{path}: expected one explicit package version")
        replacements.append((path, updated))
    return replacements


def bump(version):
    for path, updated in crate_updates(version):
        path.write_text(updated)


def check_lock(before, after, version):
    old = tomllib.loads(Path(before).read_text())
    new = tomllib.loads(Path(after).read_text())
    for lock in (old, new):
        for name in CRATES:
            packages = [p for p in lock["package"] if p["name"] == name and "source" not in p]
            if len(packages) != 1:
                raise ValueError(f"expected one workspace {name} in Cargo.lock")
            if lock is new and packages[0]["version"] != version:
                raise ValueError(f"Cargo.lock {name} does not match {version}")
            packages[0]["version"] = "WORKSPACE"
    if old != new:
        raise ValueError("Cargo.lock refresh changed more than the workspace versions")


def check_inventory(names):
    actual = set(names)
    if actual != ASSETS or len(names) != len(ASSETS):
        raise ValueError(
            f"asset inventory mismatch: missing {sorted(ASSETS - actual)}, "
            f"unexpected {sorted(actual - ASSETS)} or duplicate names"
        )


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def check_assets(directory):
    directory = Path(directory)
    check_inventory([path.name for path in directory.iterdir()])
    core_floor(directory / "release-manifest.json")
    for name in sorted(ARCHIVES):
        archive = directory / name
        sidecar = archive.with_suffix(".zip.sha256").read_text().strip()
        match = re.fullmatch(r"([a-fA-F0-9]{64}) [ *](.+)", sidecar)
        if not match or match[2] != name:
            raise ValueError(f"{name}.sha256 must name {name}")
        if digest(archive) != match[1].lower():
            raise ValueError(f"{name}: ZIP checksum mismatch")
        binary = "ck-astrocyte.exe" if "-windows-" in name else "ck-astrocyte"
        with zipfile.ZipFile(archive) as bundle:
            if bundle.namelist() != [binary] or bundle.getinfo(binary).file_size == 0:
                raise ValueError(f"{name} must contain only the nonempty root {binary}")
            if bundle.testzip() is not None:
                raise ValueError(f"{name}: ZIP member checksum mismatch")
    print(f"release: checked all {len(ARCHIVES)} ZIPs, sidecars and release-manifest.json")


def releases_for(repo, tag):
    # The by-tag release API is for published releases. With contents write
    # permission, the list endpoint also includes drafts.
    pages = json.loads(subprocess.check_output(
        ["gh", "api", "--paginate", "--slurp", f"repos/{repo}/releases?per_page=100"], text=True
    ))
    return [release for page in pages for release in page if release["tag_name"] == tag]


def check_new_draft(repo, tag):
    if releases_for(repo, tag):
        raise ValueError(f"{tag} already has a GitHub release or draft; inspect it before retrying")


def verify_draft(repo, tag, directory=None):
    releases = releases_for(repo, tag)
    if len(releases) != 1 or not releases[0]["draft"]:
        raise ValueError(f"expected exactly one unpublished draft for {tag}")
    release = releases[0]
    assets = release["assets"]
    check_inventory([asset["name"] for asset in assets])
    for asset in assets:
        name = asset["name"]
        if asset["state"] != "uploaded" or asset["size"] <= 0:
            raise ValueError(f"{name}: draft asset is incomplete")
        if directory is not None:
            path = Path(directory) / name
            # Draft browser URLs are not public. Read the asset back by id
            # with the same authenticated token that staged the draft.
            with subprocess.Popen(
                ["gh", "api", "-H", "Accept: application/octet-stream",
                 f"repos/{repo}/releases/assets/{asset['id']}"], stdout=subprocess.PIPE
            ) as download:
                uploaded = hashlib.file_digest(download.stdout, "sha256").hexdigest()
                if download.wait() != 0 or uploaded != digest(path):
                    raise ValueError(f"{name}: uploaded bytes do not match verified local asset")
    print(f"release: draft has all {len(assets)} assets: {release['html_url']}")


def publication_status(repo, tag):
    releases = releases_for(repo, tag)
    if not releases:
        print(f"GitHub release or draft for {tag}: absent")
    for release in releases:
        state = "draft" if release["draft"] else "published"
        print(f"GitHub {state}: {release['html_url']}")
        for asset in release["assets"]:
            print(f"  {asset['name']}: {asset['state']}")


def main():
    command, *args = sys.argv[1:]
    if command == "check-version":
        check_version(args[0], sys.stdin.read())
    elif command == "tag":
        if not args[0].startswith("v"):
            raise ValueError("tag must be vMAJOR.MINOR.PATCH")
        semver(args[0][1:])
    elif command == "floor":
        print(core_floor(args[0]))
    elif command == "assets":
        check_assets(*args)
    elif command == "check-new-draft":
        check_new_draft(*args)
    elif command == "verify-draft":
        verify_draft(*args)
    elif command == "bump":
        bump(*args)
    elif command == "check-crates":
        crate_updates(*args)
    elif command == "check-lock":
        check_lock(*args)
    elif command == "publication":
        publication_status(*args)
    else:
        raise ValueError(f"unknown command {command}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError, zipfile.BadZipFile) as error:
        print(f"release: {error}", file=sys.stderr)
        sys.exit(2)
