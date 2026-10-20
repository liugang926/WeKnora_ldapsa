#!/usr/bin/env python3
"""Wait for the exact merged-main GitHub Actions runs required for a release."""

import argparse
import dataclasses
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request


API_ROOT = "https://api.github.com"
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
MAX_PAGES = 10

# These workflows run on every main push and make discovery fail closed even
# when all path-filtered workflows are absent for a particular commit.
ALWAYS_REQUIRED = {
    ".github/workflows/app-image-pr.yml",
    ".github/workflows/docker-image.yml",
}

# A known workflow is accepted only when its materialized jobs retain these
# exact names and all of them succeed. Path-filtered workflows are required
# when GitHub creates a main-push run for the candidate commit.
KNOWN_REQUIRED_JOBS = {
    ".github/workflows/app.yml": {
        "Format, vet, test, and build",
        "SQLite FTS5 and PostgreSQL source contracts",
    },
    ".github/workflows/anydoc.yml": {
        "anydoc parser engine",
    },
    ".github/workflows/app-image-pr.yml": {
        "Full AnyDoc app image (amd64)",
    },
    ".github/workflows/frontend.yml": {
        "Test, type-check, and build",
        "Docker multi-stage image",
    },
    ".github/workflows/ldap-directory.yml": {
        "Isolated LDAPS and StartTLS protocol",
        "PostgreSQL and SQLite directory migrations",
    },
    ".github/workflows/go-lint-cache.yml": {
        "warm lint cache (.)",
        "warm lint cache (cli)",
        "warm lint cache (client)",
    },
    ".github/workflows/docker-image.yml": {
        "publication-policy",
    },
}

# The dsh plugin is an optional developer-harness integration that is not
# shipped by the WeKnora production image. Its pinned upstream harness has a
# known incompatibility that has failed every main push since the submission
# repository imported upstream history. Keep that independent ecosystem check
# visible in Actions without allowing it to permanently block server releases.
IGNORED_OPTIONAL_WORKFLOWS = {
    ".github/workflows/dsh-plugin.yml",
}


class GateRefused(RuntimeError):
    """A sanitized, fail-closed release-gate error."""


class GatePending(RuntimeError):
    """The exact candidate's main-push checks are not complete yet."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        return None


@dataclasses.dataclass(frozen=True)
class Snapshot:
    fingerprint: tuple
    workflow_paths: tuple


def _workflow_path(value):
    if not isinstance(value, str):
        return None
    path = value.split("@", 1)[0]
    if not re.fullmatch(r"\.github/workflows/[A-Za-z0-9._-]+\.ya?ml", path):
        return None
    return path


def select_exact_main_runs(runs, sha):
    """Select the newest attempt for each exact-SHA main push workflow."""
    selected = {}
    for run in runs:
        if not isinstance(run, dict):
            continue
        if run.get("head_sha") != sha or run.get("event") != "push" or run.get("head_branch") != "main":
            continue
        path = _workflow_path(run.get("path"))
        run_id = run.get("id")
        attempt = run.get("run_attempt", 1)
        if path is None or type(run_id) is not int or run_id <= 0 or type(attempt) is not int or attempt <= 0:
            raise GateRefused("Exact main workflow metadata is invalid")
        previous = selected.get(path)
        if previous is None or (run_id, attempt) > (previous["id"], previous.get("run_attempt", 1)):
            selected[path] = run
    return selected


def _validate_required_jobs(path, jobs):
    required = KNOWN_REQUIRED_JOBS.get(path)
    if required is None:
        return
    by_name = {}
    for job in jobs:
        if not isinstance(job, dict) or not isinstance(job.get("name"), str):
            raise GateRefused("Exact main job metadata is invalid")
        by_name.setdefault(job["name"], []).append(job)
    for name in required:
        matches = by_name.get(name, [])
        if len(matches) != 1:
            raise GateRefused("A required exact main job is missing or ambiguous")
        job = matches[0]
        if job.get("status") != "completed":
            raise GatePending("A required exact main job is still running")
        if job.get("conclusion") != "success":
            raise GateRefused("A required exact main job did not succeed")


def validate_snapshot(runs, sha, job_loader):
    """Validate one API snapshot and return its stable discovery identity."""
    selected = select_exact_main_runs(runs, sha)
    selected = {
        path: run for path, run in selected.items()
        if path not in IGNORED_OPTIONAL_WORKFLOWS
    }
    missing = ALWAYS_REQUIRED - set(selected)
    if missing:
        raise GatePending("Waiting for unconditional exact main workflows")
    for path, run in selected.items():
        if run.get("status") != "completed":
            raise GatePending("An exact main workflow is still running")
        if run.get("conclusion") != "success":
            raise GateRefused("An exact main workflow did not succeed")
        _validate_required_jobs(path, job_loader(run["id"]))
    fingerprint = tuple(sorted(
        (path, run["id"], run.get("run_attempt", 1))
        for path, run in selected.items()
    ))
    return Snapshot(fingerprint=fingerprint, workflow_paths=tuple(sorted(selected)))


class GitHubActions:
    def __init__(self, repository, token):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository or ""):
            raise GateRefused("Invalid GitHub repository identity")
        if not token:
            raise GateRefused("GitHub Actions read token is unavailable")
        self.repository = repository
        self.token = token
        self.opener = urllib.request.build_opener(NoRedirect())

    def _get(self, endpoint, query):
        encoded = urllib.parse.urlencode(query)
        url = f"{API_ROOT}/repos/{self.repository}/{endpoint}?{encoded}"
        request = urllib.request.Request(url, headers={
            "Accept": "application/vnd.github+json",
            "Authorization": "Bearer " + self.token,
            "User-Agent": "weknora-production-main-ci-gate",
            "X-GitHub-Api-Version": "2022-11-28",
        })
        try:
            with self.opener.open(request, timeout=30) as response:
                body = response.read(MAX_RESPONSE_BYTES + 1)
            if len(body) > MAX_RESPONSE_BYTES:
                raise ValueError()
            value = json.loads(body)
            if not isinstance(value, dict):
                raise ValueError()
            return value
        except Exception:
            # Never include request headers, URLs or exception text: Actions
            # diagnostics can contain credentials or attacker-controlled data.
            raise GateRefused("GitHub Actions API request failed") from None

    def _pages(self, endpoint, key, query):
        result = []
        for page in range(1, MAX_PAGES + 1):
            value = self._get(endpoint, dict(query, per_page=100, page=page))
            items = value.get(key)
            if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
                raise GateRefused("GitHub Actions API response is invalid")
            result.extend(items)
            if len(items) < 100:
                return result
        raise GateRefused("GitHub Actions API pagination bound exceeded")

    def runs(self, sha):
        return self._pages("actions/runs", "workflow_runs", {
            "head_sha": sha,
            "event": "push",
            "exclude_pull_requests": "true",
        })

    def jobs(self, run_id):
        if type(run_id) is not int or run_id <= 0:
            raise GateRefused("Invalid GitHub Actions run identity")
        return self._pages(f"actions/runs/{run_id}/jobs", "jobs", {"filter": "latest"})


def wait_for_gate(client, sha, timeout_seconds, poll_seconds, settle_seconds,
                  clock=time.monotonic, sleeper=time.sleep):
    """Wait until a complete exact-main snapshot remains unchanged."""
    deadline = clock() + timeout_seconds
    stable_fingerprint = None
    stable_since = None
    while True:
        now = clock()
        if now >= deadline:
            raise GateRefused("Timed out waiting for exact main CI")
        try:
            snapshot = validate_snapshot(client.runs(sha), sha, client.jobs)
        except GatePending:
            stable_fingerprint = None
            stable_since = None
        else:
            if snapshot.fingerprint != stable_fingerprint:
                stable_fingerprint = snapshot.fingerprint
                stable_since = now
            elif now - stable_since >= settle_seconds:
                return snapshot
        sleeper(min(poll_seconds, max(0.0, deadline - clock())))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--sha", default=os.environ.get("GITHUB_SHA"))
    parser.add_argument("--timeout-seconds", type=int, default=4500)
    parser.add_argument("--poll-seconds", type=int, default=15)
    parser.add_argument("--settle-seconds", type=int, default=60)
    args = parser.parse_args()
    try:
        if not re.fullmatch(r"[0-9a-f]{40}", args.sha or ""):
            raise GateRefused("Invalid release commit identity")
        if not 60 <= args.timeout_seconds <= 7200:
            raise GateRefused("Invalid release gate timeout")
        if not 1 <= args.poll_seconds <= 60 or not 15 <= args.settle_seconds <= 300:
            raise GateRefused("Invalid release gate polling policy")
        client = GitHubActions(args.repository, os.environ.get("GITHUB_TOKEN"))
        snapshot = wait_for_gate(client, args.sha, args.timeout_seconds,
                                 args.poll_seconds, args.settle_seconds)
        print(f"Exact merged-main CI verified for {args.sha}: {len(snapshot.workflow_paths)} workflows")
        return 0
    except GateRefused as error:
        print(str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
