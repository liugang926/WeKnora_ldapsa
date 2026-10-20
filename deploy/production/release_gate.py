#!/usr/bin/env python3
"""Wait for every exact-commit main push test workflow before release."""
import argparse
import json
import subprocess
import time
import urllib.parse

SELF_PATH = '.github/workflows/production-release.yml'
SENTINEL_PATH = '.github/workflows/app-image-pr.yml'


def relevant_runs(runs, run_id, commit):
    selected = []
    for run in runs:
        if str(run.get('id')) == str(run_id):
            continue
        if run.get('event') != 'push' or run.get('head_sha') != commit:
            continue
        if run.get('head_branch') != 'main':
            continue
        if run.get('path') == SELF_PATH:
            continue
        selected.append(run)
    return selected


def evaluate(runs, run_id, commit):
    selected = relevant_runs(runs, run_id, commit)
    failed = [
        run for run in selected
        if run.get('status') == 'completed'
        and run.get('conclusion') not in (None, 'success')
    ]
    if failed:
        names = ', '.join(
            run.get('name') or run.get('path') or str(run.get('id'))
            for run in failed
        )
        raise RuntimeError('Exact-commit main test workflow failed: ' + names)
    sentinel = [run for run in selected if run.get('path') == SENTINEL_PATH]
    if not sentinel:
        return 'waiting-for-sentinel'
    if any(run.get('status') != 'completed' for run in selected):
        return 'waiting-for-tests'
    if any(run.get('conclusion') != 'success' for run in selected):
        return 'waiting-for-tests'
    return 'ready'


def list_runs(repo, commit):
    query = urllib.parse.urlencode({
        'head_sha': commit,
        'event': 'push',
        'per_page': 100,
    })
    last_error = None
    for attempt in range(3):
        try:
            output = subprocess.check_output(
                ['gh', 'api', f'repos/{repo}/actions/runs?{query}'],
                text=True,
                stderr=subprocess.DEVNULL,
            )
            return json.loads(output).get('workflow_runs', [])
        except (subprocess.CalledProcessError, json.JSONDecodeError) as error:
            last_error = error
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
    raise RuntimeError('Unable to read exact-commit workflow runs') from last_error


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo', required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--commit', required=True)
    parser.add_argument('--attempts', type=int, default=180)
    parser.add_argument('--interval', type=int, default=30)
    args = parser.parse_args()
    for attempt in range(args.attempts):
        state = evaluate(list_runs(args.repo, args.commit), args.run_id, args.commit)
        print(f'{state} (attempt {attempt + 1}/{args.attempts})', flush=True)
        if state == 'ready':
            return
        time.sleep(args.interval)
    raise RuntimeError('Timed out waiting for the exact-commit main test workflows')


if __name__ == '__main__':
    main()
