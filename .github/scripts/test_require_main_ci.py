import pathlib
import sys
import unittest
from unittest import mock


sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import require_main_ci as gate


SHA = "a" * 40


def workflow(path, run_id, *, sha=SHA, branch="main", event="push",
             status="completed", conclusion="success", attempt=1):
    return {
        "id": run_id,
        "path": path,
        "head_sha": sha,
        "head_branch": branch,
        "event": event,
        "status": status,
        "conclusion": conclusion,
        "run_attempt": attempt,
    }


def job(name, *, status="completed", conclusion="success"):
    return {"name": name, "status": status, "conclusion": conclusion}


def current_candidate():
    runs = []
    jobs = {}
    for run_id, (path, names) in enumerate(gate.KNOWN_REQUIRED_JOBS.items(), 100):
        runs.append(workflow(path, run_id))
        jobs[run_id] = [job(name) for name in sorted(names)]
    return runs, jobs


class SnapshotTests(unittest.TestCase):
    def test_current_seven_workflow_candidate_passes_exact_jobs(self):
        runs, jobs = current_candidate()
        docker = next(run for run in runs if run["path"] == ".github/workflows/docker-image.yml")
        jobs[docker["id"]].extend([
            job("build-ui", conclusion="skipped"),
            job("build-docreader", conclusion="skipped"),
            job("build-sandbox", conclusion="skipped"),
            job("build-app", conclusion="skipped"),
            job("merge", conclusion="skipped"),
        ])
        snapshot = gate.validate_snapshot(runs, SHA, jobs.__getitem__)
        self.assertEqual(set(snapshot.workflow_paths), set(gate.KNOWN_REQUIRED_JOBS))

    def test_path_filtered_workflows_may_be_absent(self):
        runs, jobs = current_candidate()
        runs = [run for run in runs if run["path"] in gate.ALWAYS_REQUIRED]
        snapshot = gate.validate_snapshot(runs, SHA, jobs.__getitem__)
        self.assertEqual(set(snapshot.workflow_paths), gate.ALWAYS_REQUIRED)

    def test_wrong_sha_pr_and_tag_runs_cannot_satisfy_main_gate(self):
        runs, jobs = current_candidate()
        core = [run for run in runs if run["path"] in gate.ALWAYS_REQUIRED]
        altered = []
        for index, run in enumerate(core):
            if index == 0:
                altered.append(dict(run, head_sha="b" * 40))
            else:
                altered.append(dict(run, head_branch="prod-test", event="push"))
        altered.append(workflow(".github/workflows/app-image-pr.yml", 999, event="pull_request"))
        with self.assertRaises(gate.GatePending):
            gate.validate_snapshot(altered, SHA, jobs.get)

    def test_any_exact_main_workflow_failure_or_cancellation_refuses(self):
        runs, jobs = current_candidate()
        for conclusion in ("failure", "cancelled", "timed_out"):
            candidate = list(runs) + [workflow(
                ".github/workflows/rag-fixture.yml", 900,
                conclusion=conclusion,
            )]
            with self.subTest(conclusion=conclusion), self.assertRaises(gate.GateRefused):
                gate.validate_snapshot(candidate, SHA, jobs.get)

    def test_optional_dsh_harness_failure_does_not_block_server_release(self):
        runs, jobs = current_candidate()
        candidate = list(runs) + [workflow(
            ".github/workflows/dsh-plugin.yml", 901,
            conclusion="failure",
        )]
        snapshot = gate.validate_snapshot(candidate, SHA, jobs.get)
        self.assertNotIn(".github/workflows/dsh-plugin.yml", snapshot.workflow_paths)

    def test_unknown_exact_main_workflow_is_admitted_only_on_success(self):
        runs, jobs = current_candidate()
        candidate = list(runs) + [workflow(".github/workflows/new-check.yml", 900)]
        jobs[900] = [job("future validation")]
        snapshot = gate.validate_snapshot(candidate, SHA, jobs.__getitem__)
        self.assertIn(".github/workflows/new-check.yml", snapshot.workflow_paths)

    def test_running_exact_main_workflow_waits(self):
        runs, jobs = current_candidate()
        candidate = list(runs) + [workflow(
            ".github/workflows/rag-fixture.yml", 900,
            status="in_progress", conclusion=None,
        )]
        with self.assertRaises(gate.GatePending):
            gate.validate_snapshot(candidate, SHA, jobs.get)

    def test_missing_ambiguous_or_skipped_required_job_refuses(self):
        runs, jobs = current_candidate()
        app = next(run for run in runs if run["path"] == ".github/workflows/app.yml")
        required = sorted(gate.KNOWN_REQUIRED_JOBS[app["path"]])
        cases = [
            [job(required[0])],
            [job(name) for name in required] + [job(required[0])],
            [job(required[0], conclusion="skipped"), job(required[1])],
        ]
        for replacement in cases:
            changed = dict(jobs)
            changed[app["id"]] = replacement
            with self.subTest(replacement=replacement), self.assertRaises(gate.GateRefused):
                gate.validate_snapshot(runs, SHA, changed.__getitem__)

    def test_newest_run_identity_wins_over_an_older_rerun(self):
        runs, jobs = current_candidate()
        path = ".github/workflows/app-image-pr.yml"
        old = next(run for run in runs if run["path"] == path)
        old["run_attempt"] = 3
        newer = dict(old, id=777, run_attempt=1)
        jobs[777] = [job("Full AnyDoc app image (amd64)")]
        snapshot = gate.validate_snapshot(runs + [newer], SHA, jobs.__getitem__)
        self.assertIn((path, 777, 1), snapshot.fingerprint)


class WaitingTests(unittest.TestCase):
    class Clock:
        def __init__(self):
            self.value = 0.0

        def now(self):
            return self.value

        def sleep(self, seconds):
            self.value += seconds

    class Client:
        def __init__(self, snapshots, jobs):
            self.snapshots = list(snapshots)
            self.job_map = jobs
            self.calls = 0

        def runs(self, _sha):
            index = min(self.calls, len(self.snapshots) - 1)
            self.calls += 1
            return self.snapshots[index]

        def jobs(self, run_id):
            return self.job_map[run_id]

    def test_discovery_window_restarts_when_new_workflow_appears(self):
        runs, jobs = current_candidate()
        core = [run for run in runs if run["path"] in gate.ALWAYS_REQUIRED]
        clock = self.Clock()
        client = self.Client([core, runs, runs, runs], jobs)
        snapshot = gate.wait_for_gate(
            client, SHA, timeout_seconds=100, poll_seconds=10,
            settle_seconds=20, clock=clock.now, sleeper=clock.sleep,
        )
        self.assertEqual(set(snapshot.workflow_paths), set(gate.KNOWN_REQUIRED_JOBS))
        self.assertEqual(clock.value, 30)
        self.assertEqual(client.calls, 4)

    def test_timeout_fails_closed(self):
        runs, jobs = current_candidate()
        pending = [dict(runs[0], status="in_progress", conclusion=None)]
        clock = self.Clock()
        client = self.Client([pending], jobs)
        with self.assertRaisesRegex(gate.GateRefused, "Timed out"):
            gate.wait_for_gate(
                client, SHA, timeout_seconds=30, poll_seconds=10,
                settle_seconds=20, clock=clock.now, sleeper=clock.sleep,
            )


class APIClientTests(unittest.TestCase):
    def test_redirects_are_not_followed_with_the_actions_token(self):
        handler = gate.NoRedirect()
        self.assertIsNone(handler.redirect_request(None, None, 302, "", {}, "https://example.invalid"))

    def test_network_errors_are_sanitized_and_never_print_token(self):
        secret = "private-test-token"
        client = gate.GitHubActions("owner/repository", secret)
        with mock.patch.object(client.opener, "open", side_effect=RuntimeError(secret)):
            with self.assertRaises(gate.GateRefused) as caught:
                client.runs(SHA)
        self.assertEqual(str(caught.exception), "GitHub Actions API request failed")
        self.assertNotIn(secret, str(caught.exception))


if __name__ == "__main__":
    unittest.main()
