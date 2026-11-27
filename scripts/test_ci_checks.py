"""Offline regressions for the train workflow and its local checks."""

import contextlib
import hashlib
import io
import os
import pathlib
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import check_doc_references as docs
import check_train_preconditions as train

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRATCH = ROOT / "target/scratch"


class GateChecks(unittest.TestCase):
    def test_default_doc_check_requires_the_surface_operation_inventory(self):
        names = (*docs.OVERVIEW_FILES, docs.TABLE_INVENTORY_FILE, "docs/module-surface.md")
        fixtures = {name: (ROOT / name).read_text() for name in names}
        files, dirs = docs.tracked_paths()
        source = docs.source_text()
        operations = docs.served_operations()
        chain = docs.migrations()
        SCRATCH.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=SCRATCH) as directory:
            root = pathlib.Path(directory)
            for name, text in fixtures.items():
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text)

            def check():
                output = io.StringIO()
                with (
                    patch.object(docs, "ROOT", root),
                    patch.object(docs, "tracked_paths", return_value=(files, dirs)),
                    patch.object(docs, "source_text", return_value=source),
                    patch.object(docs, "served_operations", return_value=operations),
                    patch.object(docs, "migrations", return_value=chain),
                    contextlib.redirect_stdout(output),
                ):
                    status = docs.main(["check_doc_references.py"])
                return status, output.getvalue()

            self.assertEqual(check()[0], 0)
            surface = root / "docs/module-surface.md"
            original = surface.read_text()
            for changed in [
                "\n".join(line for line in original.splitlines() if not line.startswith("| `budget.verdict` |")),
                original.replace("| Operation |", "| Removed heading |"),
            ]:
                surface.write_text(changed)
                status, output = check()
                self.assertEqual(status, 1, output)
                self.assertIn("docs/module-surface.md: operation list differs", output)
                self.assertIn("budget.verdict", output)

    def test_wrong_yaml_version_is_refused(self):
        output = io.StringIO()
        with patch.object(train.yaml, "__version__", "0.0"), contextlib.redirect_stderr(output):
            self.assertEqual(train.main(), 1)
        self.assertIn("PyYAML==", output.getvalue())

    def test_native_macos_job_runs_tests_on_train_pushes(self):
        workflow = train.yaml.safe_load(train.WORKFLOW.read_text())
        errors, branches, _, _ = train.check(workflow)
        self.assertEqual(errors, [])
        self.assertIn("train/**", branches)
        macos_jobs = [job for job in workflow["jobs"].values() if job["runs-on"].startswith("macos-")]
        self.assertEqual(len(macos_jobs), 1)
        job = macos_jobs[0]
        self.assertNotIn("if", job)
        self.assertEqual(job["env"]["ASTROCYTE_REQUIRE_NATS_SERVER"], "1")
        self.assertTrue(any(step.get("run", "").startswith("cargo test --workspace --all-targets --locked")
                            and "if" not in step for step in job["steps"]))

    def test_nats_installer_only_accepts_the_pinned_archive(self):
        SCRATCH.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=SCRATCH) as directory:
            root = pathlib.Path(directory)
            release = "nats-server-v2.15.0-darwin-arm64"
            contents = b"#!/usr/bin/env bash\necho 'nats-server: v2.15.0'\n"
            with tarfile.open(root / "release.tgz", "w:gz") as fixture:
                binary = tarfile.TarInfo(f"{release}/nats-server")
                binary.mode = 0o755
                binary.size = len(contents)
                fixture.addfile(binary, io.BytesIO(contents))
            fake_curl = root / "curl"
            fake_curl.write_text(f'''#!/usr/bin/env bash
set -eu
case "$*" in *"/v2.15.0/{release}.tar.gz"*) ;; *) exit 2 ;; esac
cp "$(dirname "$0")/release.tgz" "${{@: -1}}"
''')
            fake_curl.chmod(0o755)
            env = dict(os.environ, NATS_SERVER_VERSION="2.15.0", PATH=f"{directory}{os.pathsep}{os.environ['PATH']}")
            destination = root / "installed"
            command = ["bash", "scripts/lib/install-pinned-nats.sh", "darwin-arm64", str(destination)]
            invalid = subprocess.run(command, cwd=ROOT, env=dict(env, NATS_SERVER_SHA256="0" * 64),
                                     capture_output=True, text=True, timeout=15)
            self.assertNotEqual(invalid.returncode, 0, invalid.stdout)
            self.assertFalse((destination / "nats-server").exists())
            checksum = hashlib.sha256((root / "release.tgz").read_bytes()).hexdigest()
            installed = subprocess.run(command, cwd=ROOT, env=dict(env, NATS_SERVER_SHA256=checksum),
                                       capture_output=True, text=True, timeout=15)
            self.assertEqual(installed.returncode, 0, installed.stderr)
            self.assertIn("nats-server: v2.15.0", installed.stdout)
            self.assertEqual((destination / "nats-server").read_bytes(), contents)


class WatchChecks(unittest.TestCase):
    def watch(self, replies, *args, workflow=None):
        SCRATCH.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=SCRATCH) as directory:
            root = pathlib.Path(directory)
            for index, reply in enumerate(replies, start=1):
                (root / f"reply-{index}").write_text(reply)
            gh = root / "gh"
            gh.write_text('''#!/usr/bin/env bash
set -eu
cd "$(dirname "$0")"
if [ "$2" = list ]; then
  printf '%s\n' "$*" > list-args
  echo 12345
  exit
fi
n=0
[ ! -f count ] || read -r n < count
n=$((n + 1))
echo "$n" > count
# The fallback lets a broken watcher stop instead of hanging the test.
if [ ! -f "reply-$n" ]; then echo 'fixture replies exhausted' >&2; exit 1; fi
cat "reply-$n"
''')
            gh.chmod(0o755)
            env = {key: value for key, value in os.environ.items() if not key.startswith("WATCH_CI_")}
            env.update(REPO="test/project", WATCH_CI_POLL_SLEEP="0", WATCH_CI_RESOLVE_SLEEP="0",
                       TMPDIR=directory, PATH=f"{directory}{os.pathsep}{env['PATH']}")
            if workflow:
                env["WATCH_CI_WORKFLOW"] = workflow
            result = subprocess.run(["bash", "scripts/watch-ci.sh", *args], cwd=ROOT, env=env,
                                    capture_output=True, text=True, timeout=15)
            count = int((root / "count").read_text()) if (root / "count").exists() else 0
            lookup = (root / "list-args").read_text() if (root / "list-args").exists() else ""
            return result.returncode, result.stdout + result.stderr, count, lookup

    def test_watch_resolves_sha_using_the_default_ci_workflow(self):
        statuses = [
            '{"status":"in_progress","jobs":[{"name":"Test (ubuntu)","conclusion":"success"},'
            '{"name":"Test (windows)","conclusion":"success"},{"name":"Test (macOS)"}]}',
            '{"status":"completed","conclusion":"success","jobs":[{"name":"Test (ubuntu)","conclusion":"success"},'
            '{"name":"Test (windows)","conclusion":"success"},{"name":"Test (macOS)","conclusion":"success"}]}',
        ]
        code, output, count, lookup = self.watch(statuses, "HEAD")
        self.assertEqual(code, 0, output)
        self.assertEqual(count, 2)
        self.assertIn("--workflow ci.yml --event push", lookup)
        self.assertIn("CI_DONE run=12345", output)

    def test_watch_stops_after_four_malformed_run_views(self):
        replies = ["not JSON", "{}", '{"status":"in_progress"}', '{"status":"invalid","jobs":[]}']
        code, output, count, _ = self.watch(replies, "12345", workflow="ci.yml")
        self.assertEqual(code, 3, output)
        self.assertEqual(count, 4)
        self.assertIn("CI_UNDETERMINED run=12345", output)

    def test_valid_run_view_resets_the_malformed_reply_budget(self):
        replies = ["not JSON"] * 3 + ['{"status":"in_progress","jobs":[]}'] + ["{}"] * 3
        replies.append('{"status":"completed","conclusion":"success","jobs":[{"conclusion":"success"}]}')
        code, output, count, lookup = self.watch(replies, "12345")
        self.assertEqual(code, 0, output)
        self.assertEqual(count, len(replies))
        self.assertEqual(lookup, "", "numeric run IDs do not need a workflow lookup")


if __name__ == "__main__":
    unittest.main()
