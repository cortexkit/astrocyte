#!/usr/bin/env python3
"""Offline release checks; no GitHub queries, tag writes or Cargo builds."""

import hashlib
import io
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import release
import release_ci


ROOT = Path(__file__).resolve().parents[2]
HELPER = ROOT / "scripts/release/release.py"


class ReleaseChecks(unittest.TestCase):
    def setUp(self):
        scratch = ROOT / "target/scratch"
        scratch.mkdir(parents=True, exist_ok=True)
        temp = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(temp.cleanup)
        self.directory = Path(temp.name)

    def test_release_candidate_without_core_floor_is_refused(self):
        # Mirror only the release driver and manifest check. No Git repository
        # or credentials are needed to refuse this release candidate.
        helper = self.directory / "scripts/release/release.py"
        helper.parent.mkdir(parents=True)
        shutil.copyfile(HELPER, helper)
        driver = self.directory / "scripts/release.sh"
        shutil.copyfile(ROOT / "scripts/release.sh", driver)
        manifest = self.directory / "release-manifest.json"
        manifest.write_text("{}\n")
        result = subprocess.run(
            ["bash", driver, "1.0.0", "--dry-run"], capture_output=True, text=True, timeout=10
        )
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("requires_core must be a verified MAJOR.MINOR.PATCH", result.stderr)
        self.assertIn("no release tag or GitHub draft was created", result.stderr)
        self.assertFalse((self.directory / "target").exists())

    def test_manifest_floor_checks_type_and_format(self):
        manifest = self.directory / "release-manifest.json"
        for invalid in ("null", "21", '"v0.21.3"', '"01.21.3"', '"0.21.3-alpha"'):
            with self.subTest(invalid=invalid):
                manifest.write_text('{"requires_core":' + invalid + '}')
                with self.assertRaisesRegex(ValueError, "requires_core must be"):
                    release.core_floor(manifest)
        manifest.write_text('{"requires_core":"0.21.3"}')
        self.assertEqual(release.core_floor(manifest), "0.21.3")

    def test_tag_requires_v_prefix_and_plain_version(self):
        for tag, expected in (("v0.1.0", 0), ("0.1.0", 2), ("vv0.1.0", 2), ("v0.1.0-rc.1", 2)):
            with self.subTest(tag=tag):
                result = subprocess.run([sys.executable, HELPER, "tag", tag], capture_output=True, timeout=10)
                self.assertEqual(result.returncode, expected)

    def write_archive(self, target, *members):
        archive = self.directory / f"ck-astrocyte-{target}.zip"
        with zipfile.ZipFile(archive, "w") as bundle:
            for member in members:
                bundle.writestr(member, "binary fixture")
        checksum = hashlib.sha256(archive.read_bytes()).hexdigest()
        archive.with_suffix(".zip.sha256").write_text(f"{checksum}  {archive.name}\n")
        return archive

    def write_inventory(self):
        for target in ("darwin-arm64", "linux-x64", "linux-arm64", "windows-x64", "windows-arm64"):
            self.write_archive(target, "ck-astrocyte.exe" if target.startswith("windows") else "ck-astrocyte")
        (self.directory / "release-manifest.json").write_text('{"requires_core":"0.21.3"}')

    def test_exact_inventory_sidecars_and_root_executable(self):
        self.write_inventory()
        release.check_assets(self.directory)

        archive = self.directory / "ck-astrocyte-windows-arm64.zip"
        archive.unlink()
        with self.assertRaisesRegex(ValueError, "missing.*windows-arm64.zip"):
            release.check_assets(self.directory)

        self.write_archive("windows-arm64", "ck-astrocyte.exe", "corpus_drive.exe")
        with self.assertRaisesRegex(ValueError, "only the nonempty root ck-astrocyte.exe"):
            release.check_assets(self.directory)

        self.write_archive("windows-arm64", "ck-astrocyte.exe")
        archive.write_bytes(archive.read_bytes() + b"tampered")
        with self.assertRaisesRegex(ValueError, "windows-arm64.zip: ZIP checksum mismatch"):
            release.check_assets(self.directory)

    def test_uploaded_draft_is_complete_and_matches_local_bytes(self):
        self.write_inventory()
        paths = sorted(self.directory.iterdir())
        assets = [dict(id=i, name=p.name, state="uploaded", size=p.stat().st_size) for i, p in enumerate(paths)]
        draft = dict(draft=True, assets=assets, html_url="https://example.invalid/draft")
        wrong_asset = None

        def download(args, *, stdout):
            self.assertEqual(stdout, subprocess.PIPE)
            self.assertEqual(args[:4], ["gh", "api", "-H", "Accept: application/octet-stream"])
            path = paths[int(args[4].rsplit("/", 1)[1])]
            contents = path.read_bytes() if path.name != wrong_asset else b"different uploaded bytes"
            return FakeDownload(contents)

        with patch.object(release, "releases_for", return_value=[draft]), patch.object(
            release.subprocess, "Popen", side_effect=download
        ) as requests:
            release.verify_draft("owner/repo", "v0.1.0", self.directory)
            self.assertEqual(requests.call_count, 11)
            with self.assertRaisesRegex(ValueError, "already has a GitHub release or draft"):
                release.check_new_draft("owner/repo", "v0.1.0")
            wrong_asset = "ck-astrocyte-windows-x64.zip.sha256"
            with self.assertRaisesRegex(ValueError, "uploaded bytes do not match"):
                release.verify_draft("owner/repo", "v0.1.0", self.directory)
            draft["draft"] = False
            with self.assertRaisesRegex(ValueError, "expected exactly one unpublished draft"):
                release.verify_draft("owner/repo", "v0.1.0", self.directory)


class FakeDownload:
    def __init__(self, contents):
        self.stdout = io.BytesIO(contents)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.stdout.close()

    def wait(self):
        return 0


class ReleaseCIChecks(unittest.TestCase):
    def test_only_exact_push_ci_with_linux_and_win_checks_qualifies(self):
        run = dict(headSha="candidate", headBranch="train/release-0.1.0", event="push",
                   status="completed", conclusion="success")
        self.assertTrue(release_ci.eligible(run, "candidate"))
        self.assertFalse(release_ci.eligible(run, "previous"))
        self.assertFalse(release_ci.eligible({**run, "event": "schedule"}, "candidate"))
        self.assertFalse(release_ci.eligible({**run, "headBranch": "feature"}, "candidate"))

        listed = (ROOT / "scripts/release/required-checks.txt").read_text()
        required = release_ci.required_checks({"contexts": ["extra-protected-check"]}, listed)
        self.assertEqual(required, {"Test (ubuntu)", "Test (windows)", "extra-protected-check"})
        jobs = [dict(name=name, status="completed", conclusion="success") for name in required]
        self.assertEqual(release_ci.missing_checks(jobs, required), [])
        windows = next(job for job in jobs if job["name"] == "Test (windows)")
        windows["conclusion"] = "skipped"
        self.assertEqual(release_ci.missing_checks(jobs, required), [
            "Test (windows): not every matching job completed successfully"
        ])


if __name__ == "__main__":
    unittest.main()
