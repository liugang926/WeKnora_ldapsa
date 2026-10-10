#!/usr/bin/env python3
"""Offline contract tests executing the actual runtime APT install helper."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "scripts/install-runtime-packages.sh"
PACKAGES = [
    "build-essential", "postgresql-client", "default-mysql-client", "tzdata",
    "sed", "curl", "bash", "vim", "wget", "libsqlite3-0", "python3",
    "python3-pip", "python3-dev", "libffi-dev", "libssl-dev", "nodejs",
    "npm", "gosu", "ffmpeg",
]
FAKE_APT = r'''#!/usr/bin/env python3
import json, os
from pathlib import Path
import sys

root = Path(os.environ["WEKNORA_APT_TEST_ROOT"])
args = sys.argv[1:]
operation = next(arg for arg in args if arg in {"install", "clean"})
cache = Path(next(arg.split("=", 1)[1] for arg in args if arg.startswith("Dir::Cache::archives=")))
record = {"operation": operation, "args": args, "cache": str(cache)}
if operation == "install":
    counter = root / "counter"
    attempt = int(counter.read_text()) + 1 if counter.exists() else 1
    counter.write_text(str(attempt))
    record["attempt"] = attempt
    record["prior_archive_present"] = (cache / "cached-1.deb").exists()
    cache.mkdir(exist_ok=True)
    (cache / ("cached-" + str(attempt) + ".deb")).write_bytes(b"fixture archive")
    # Model the official Debian hook's fixed DEFAULT path. This does not
    # replace the later real-APT fixture needed to verify APT behavior.
    default_cache = root / "default-archives"
    default_cache.mkdir(exist_ok=True)
    (default_cache / "default.deb").write_bytes(b"default cache fixture")
    for entry in default_cache.glob("*.deb"):
        entry.unlink()
    statuses = json.loads(os.environ["WEKNORA_APT_TEST_STATUSES"])
    status = statuses[min(attempt - 1, len(statuses) - 1)]
else:
    status = int(os.environ.get("WEKNORA_APT_TEST_CLEAN_STATUS", "0"))
with (root / "trace.jsonl").open("a") as stream:
    stream.write(json.dumps(record) + "\n")
raise SystemExit(status)
'''
FAKE_SLEEP = r'''#!/usr/bin/env python3
import json, os
from pathlib import Path
import sys
with (Path(os.environ["WEKNORA_APT_TEST_ROOT"]) / "trace.jsonl").open("a") as stream:
    stream.write(json.dumps({"operation": "sleep", "args": sys.argv[1:]}) + "\n")
'''


class RuntimeAPTTests(unittest.TestCase):
    def setUp(self):
        self.fixture = tempfile.TemporaryDirectory(prefix="weknora-apt-contract-")
        self.root = Path(self.fixture.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        for name, source in (("apt-get", FAKE_APT), ("sleep", FAKE_SLEEP)):
            executable = self.bin / name
            executable.write_text(source)
            executable.chmod(0o755)

    def tearDown(self):
        # Remove only helper-created temporary fixture archives left by a
        # failed test run, after the test has checked failure retention.
        for cache in {Path(row["cache"]) for row in self.trace() if "cache" in row}:
            if cache.parent == Path("/tmp") and cache.name.startswith("weknora-runtime-apt.") and cache.is_dir():
                shutil.rmtree(cache)
        self.fixture.cleanup()

    def trace(self):
        path = self.root / "trace.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def run_helper(self, statuses, *, packages=PACKAGES, clean_status=0):
        environment = os.environ.copy()
        environment.update({
            "PATH": str(self.bin) + os.pathsep + environment.get("PATH", ""),
            "WEKNORA_APT_TEST_ROOT": str(self.root),
            "WEKNORA_APT_TEST_STATUSES": json.dumps(statuses),
            "WEKNORA_APT_TEST_CLEAN_STATUS": str(clean_status),
            "PYTHONDONTWRITEBYTECODE": "1",
        })
        return subprocess.run(["bash", str(HELPER), *packages], env=environment,
                              capture_output=True, text=True, timeout=10)

    def assert_install_contract(self, rows):
        caches = {row["cache"] for row in rows}
        self.assertEqual(len(caches), 1)
        for row in rows:
            self.assertEqual(row["args"], [
                "-o", "Acquire::Retries=5", "-o", "Acquire::http::Timeout=180",
                "-o", "Dir::Cache::archives=" + row["cache"],
                "-o", "APT::Keep-Downloaded-Packages=true", "install", "-y",
                "--no-install-recommends", *PACKAGES,
            ])

    def test_first_success_cleans_once_without_sleep(self):
        result = self.run_helper([0])
        self.assertEqual(result.returncode, 0, result.stderr)
        rows = self.trace()
        self.assertEqual([row["operation"] for row in rows], ["install", "clean"])
        self.assert_install_contract(rows[:1])
        self.assertEqual(rows[1]["args"], ["-o", "Dir::Cache::archives=" + rows[0]["cache"], "clean"])
        self.assertFalse(Path(rows[0]["cache"]).exists())

    def test_failures_then_success_reuse_archives_and_clean_last(self):
        result = self.run_helper([100, 101, 0])
        self.assertEqual(result.returncode, 0, result.stderr)
        rows = self.trace()
        self.assertEqual([row["operation"] for row in rows],
                         ["install", "sleep", "install", "sleep", "install", "clean"])
        installs = [row for row in rows if row["operation"] == "install"]
        self.assert_install_contract(installs)
        self.assertEqual([row["prior_archive_present"] for row in installs], [False, True, True])
        self.assertEqual([row["args"] for row in rows if row["operation"] == "sleep"], [["2"], ["2"]])
        self.assertFalse(Path(installs[0]["cache"]).exists())

    def test_exhaustion_returns_last_exit_and_preserves_cache(self):
        result = self.run_helper([100, 101, 102, 0])
        self.assertEqual(result.returncode, 102, result.stderr)
        rows = self.trace()
        self.assertEqual([row["operation"] for row in rows],
                         ["install", "sleep", "install", "sleep", "install"])
        installs = [row for row in rows if row["operation"] == "install"]
        self.assert_install_contract(installs)
        self.assertEqual([row["attempt"] for row in installs], [1, 2, 3])
        self.assertTrue((Path(installs[-1]["cache"]) / "cached-1.deb").exists())

    def test_cleanup_failure_propagates_without_removing_owned_directory(self):
        result = self.run_helper([0], clean_status=42)
        self.assertEqual(result.returncode, 42, result.stderr)
        rows = self.trace()
        self.assertEqual([row["operation"] for row in rows], ["install", "clean"])
        # Real apt-get clean can partially remove files before failing. The
        # contract here is that the helper does not then remove its directory.
        self.assertTrue(Path(rows[0]["cache"]).is_dir())

    def test_no_packages_rejected_before_any_apt_call(self):
        result = self.run_helper([0], packages=[])
        self.assertEqual(result.returncode, 64)
        self.assertEqual(self.trace(), [])

    def test_dockerfile_keeps_signed_update_and_exact_full_package_vector(self):
        dockerfile = (ROOT / "docker/Dockerfile.app").read_text()
        self.assertIn("COPY scripts/install-runtime-packages.sh /tmp/install-runtime-packages.sh", dockerfile)
        runtime = dockerfile.split("COPY scripts/install-runtime-packages.sh", 1)[1]
        self.assertIn("apt-get -o Acquire::Retries=5 -o Acquire::http::Timeout=180 update &&", runtime)
        batch = runtime.split("bash /tmp/install-runtime-packages.sh", 1)[1].split("&&", 1)[0]
        self.assertEqual(batch.replace("\\", " ").split(), PACKAGES)
        self.assertIn("ffmpeg && \\\n    rm -rf /var/lib/apt/lists/*", runtime)
        self.assertNotIn("--fix-missing", runtime)
        self.assertNotIn("--allow-unauthenticated", runtime)


if __name__ == "__main__":
    unittest.main()
