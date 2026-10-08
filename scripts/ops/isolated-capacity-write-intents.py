#!/usr/bin/env python3
"""Verify local capacity crash/retry using read-only source and owned stock PG."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time

PG_IMAGE = 'sha256:20edbde7749f822887a1a022ad526fde0a47d6b2be9a8364433605cf65099416'
GO_IMAGE = 'sha256:5a74e8b5137496d3e22c72c01d862c4cbb1fe6ce476ddf5616fd9a807d6ae9ac'
OWNER = 'org.weknora.capacity.intent.owner'


def run(*args):
    result = subprocess.run(args, capture_output=True, text=True, timeout=600)
    if result.returncode:
        raise RuntimeError(args[0] + ' failed, exit ' + str(result.returncode))
    return result.stdout.strip()


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-dir', type=Path,
                        help='new private directory; existing directories are refused')
    args = parser.parse_args()
    os.umask(0o077)
    root = Path(__file__).resolve().parents[2]
    before_head = run('git', '-C', str(root), 'rev-parse', 'HEAD')
    before_status = run('git', '-C', str(root), 'status', '--porcelain')
    require(not before_status, 'source checkout must be clean before verification')
    if args.evidence_dir:
        output = args.evidence_dir.absolute()
        require(not output.exists() and not output.is_symlink(), 'evidence directory already exists')
        output.mkdir(mode=0o700)
        output = output.resolve()
    else:
        output = Path(tempfile.mkdtemp(prefix='capacity-intent-evidence-')).resolve()
    require(output != root and root not in output.parents,
            'evidence must be outside the source checkout')
    require(output.stat().st_uid == os.geteuid() and output.stat().st_mode & 0o777 == 0o700,
            'evidence directory must be private and owned')
    context = json.loads(run('docker', 'context', 'inspect'))[0]
    require(context['Endpoints']['docker']['Host'].startswith('unix://'), 'local Docker required')
    require(not os.environ.get('DOCKER_HOST') or os.environ['DOCKER_HOST'].startswith('unix://'),
            'remote Docker refused')
    nonce = secrets.token_hex(8)
    name = 'capacity-intent-' + nonce
    label = OWNER + '=' + nonce
    network = pg = go = None
    report = {'schema_version': 1, 'source_commit': before_head,
              'source_clean_before': True, 'status': 'INCOMPLETE', 'nonce': nonce,
              'go_image': GO_IMAGE, 'postgres_image': PG_IMAGE,
              'internal_network': True, 'host_ports': False,
              'shared_target_modified': False, 'source_mounted_read_only': True}
    try:
        network = run('docker', 'network', 'create', '--internal', '--label', label, name)
        password = 'x' + secrets.token_urlsafe(20)
        pg = run('docker', 'run', '-d', '--name', name + '-pg', '--label', label,
                 '--cpus', '1', '--memory', '512m', '--network', network,
                 '--network-alias', 'capacity-db', '--tmpfs',
                 '/var/lib/postgresql/data:size=256m,mode=700',
                 '-e', 'POSTGRES_USER=capacity', '-e', 'POSTGRES_DB=capacity',
                 '-e', 'POSTGRES_PASSWORD=' + password, PG_IMAGE)
        for _ in range(60):
            ready = subprocess.run(['docker', 'exec', pg, 'pg_isready', '-h', '127.0.0.1',
                                    '-U', 'capacity', '-d', 'capacity'], capture_output=True)
            if ready.returncode == 0:
                break
            time.sleep(.5)
        else:
            raise RuntimeError('owned PG TCP listener did not become ready')
        info = json.loads(run('docker', 'inspect', pg))[0]
        require(info['HostConfig']['Memory'] == 512 * 1024**2 and
                info['HostConfig']['NanoCpus'] == 10**9 and not info['HostConfig']['PortBindings'],
                'owned PG limits or port contract changed')
        dsn = 'host=capacity-db user=capacity dbname=capacity password=' + password + ' sslmode=disable'
        script = ('go build -buildvcs=false -p 1 -o /tmp/capacity-reconcile ./cmd/capacity-reconcile && '
                  'WEKNORA_CAPACITY_RECONCILE_CLI=/tmp/capacity-reconcile '
                  'go test -json -p 1 ./internal/application/service ./internal/application/repository '
                  './internal/application/service/file ./internal/database '
                  '-run "Test(CapacityIntent|PostgresCapacityIntent|NextcloudCapacity|PostgresNextcloudCapacity|'
                  'SQLite|MigrationDirectoriesLoad|Local|ResourceCatalog|BackendScoped|CreateKnowledgeFromFile)" '
                  '-count=1')
        go = run('docker', 'run', '-d', '--name', name + '-go', '--label', label,
                 '--cpus', '1', '--memory', '2g', '--network', network,
                 '-v', str(root) + ':/app:ro', '-v', 'weknora-go-mod:/go/pkg/mod:ro',
                 '-v', 'weknora-go-build:/root/.cache/go-build', '-w', '/app',
                 '-e', 'GOPROXY=off', '-e', 'GOSUMDB=off', '-e', 'GOTOOLCHAIN=local',
                 '-e', 'WEKNORA_CAPACITY_TEST_POSTGRES_DSN=' + dsn,
                 '-e', 'WEKNORA_LEASE_TEST_POSTGRES_DSN=' + dsn,
                 GO_IMAGE, 'sh', '-c', script)
        code = run('docker', 'wait', go)
        logs = subprocess.run(['docker', 'logs', go], capture_output=True, text=True)
        text = (logs.stdout + logs.stderr).replace(password, '[redacted]')
        with (output / 'tests.jsonl').open('x') as handle:
            handle.write(text)
        counts = dict.fromkeys(('pass', 'fail', 'skip'), 0)
        for line in text.splitlines():
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get('Test') and event.get('Action') in counts:
                counts[event['Action']] += 1
        remaining = run('docker', 'exec', pg, 'psql', '-U', 'capacity', '-d', 'capacity', '-At', '-c',
                        "SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'capacity_intent_%' OR nspname LIKE 'content_lease_%'")
        require(remaining == '0', 'owned fixture schemas were not released')
        after_head = run('git', '-C', str(root), 'rev-parse', 'HEAD')
        after_status = run('git', '-C', str(root), 'status', '--porcelain')
        require(after_head == before_head and after_status == before_status,
                'source checkout changed during verification')
        report.update(status='PASS' if code == '0' else 'FAIL', exit_code=int(code),
                      test_cases=counts, source_commit_after=after_head, source_clean_after=True,
                      tests_log_sha256=hashlib.sha256(text.encode()).hexdigest(),
                      remaining_nonce_schemas=int(remaining), go_container_id=go, pg_container_id=pg,
                      network_id=network, limits={'go': '1 CPU/2 GiB', 'postgres': '1 CPU/512 MiB/tmpfs'})
    finally:
        for cid in (go, pg):
            if cid:
                info = json.loads(run('docker', 'inspect', cid))[0]
                require(info['Config']['Labels'].get(OWNER) == nonce, 'fixture ownership changed')
                run('docker', 'rm', '-f', cid)
        if network:
            info = json.loads(run('docker', 'network', 'inspect', network))[0]
            require(info['Internal'] and info['Labels'].get(OWNER) == nonce and not info['Containers'],
                    'owned network is not empty or identity changed')
            run('docker', 'network', 'rm', network)
        report['cleanup_verified'] = True
        with (output / 'result.json').open('x') as handle:
            json.dump(report, handle, indent=2)
            handle.write('\n')
    print(json.dumps({'status': report['status'], 'evidence_dir': str(output),
                      'test_cases': report.get('test_cases'), 'cleanup_verified': True}))
    return 0 if report['status'] == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
