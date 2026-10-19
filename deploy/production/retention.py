#!/usr/bin/env python3
"""Reviewed, root-only offline journal gate; never restore or reset an anchor.

The HMAC key, append ledger and monotonic pin live outside runtime snapshots.
Only the packaged application CLI interprets/authenticates journal contents.
"""
import argparse
import contextlib
from datetime import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import stat
import subprocess
import sys
import time


class Refused(RuntimeError):
    """A deliberately secret-free diagnostic."""


# This text, not a DSN or credentials, appears in Docker's command arguments.
# Suppress even a library's unsolicited diagnostic before constructing the DSN.
CONTAINER_COMMAND = r'''
import os, sys
from urllib.parse import quote
null = os.open(os.devnull, os.O_WRONLY)
os.dup2(null, 1); os.dup2(null, 2)
os.close(null)
try:
    mode = sys.argv[1]
    if mode not in ('migrate', 'init', 'verify', 'reconcile'):
        raise ValueError()
    if os.environ.get('DB_DRIVER', 'postgres') != 'postgres':
        raise ValueError()
    port = int(os.environ.get('DB_PORT', '5432'))
    if not 0 < port < 65536:
        raise ValueError()
    host = os.environ['DB_HOST']
    if not host or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-' for c in host):
        raise ValueError()
    user, password, name = (os.environ[k] for k in ('DB_USER', 'DB_PASSWORD', 'DB_NAME'))
    if not user or not password or not name:
        raise ValueError()
    os.environ['WEKNORA_ORIGINAL_BODY_MAINTENANCE_DSN'] = (
        'postgres://' + quote(user, safe='') + ':' + quote(password, safe='') +
        '@' + host + ':' + str(port) + '/' + quote(name, safe='') + '?sslmode=disable')
    os.execv('/usr/local/bin/original-body-retention',
             ['original-body-retention', '-driver', 'postgres', '-mode', mode])
except BaseException:
    os._exit(1)
'''

IDENTITY_COMMAND = (
    'import json,pwd; p=pwd.getpwnam("appuser"); '
    'print(json.dumps([p.pw_uid,p.pw_gid]))'
)


def _no_symlinks(path):
    """Check every existing component, including the path itself."""
    path = Path(path)
    if not path.is_absolute():
        raise Refused('retention paths must be absolute')
    for part in reversed((path, *path.parents)):
        try:
            value = part.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(value.st_mode):
            raise Refused('symlink in retention path')


def _metadata(path, uid, gid, directory=False):
    _no_symlinks(path)
    try:
        value = Path(path).lstat()
    except FileNotFoundError:
        raise Refused('required retention material is missing') from None
    valid_type = stat.S_ISDIR(value.st_mode) if directory else stat.S_ISREG(value.st_mode)
    if not valid_type or (value.st_uid, value.st_gid) != (uid, gid):
        raise Refused('retention type or owner mismatch')
    if stat.S_IMODE(value.st_mode) != (0o700 if directory else 0o600):
        raise Refused('retention permission mismatch')
    if not directory and value.st_nlink != 1:
        raise Refused('retention file has multiple hard links')
    return value


def _read_private(path, uid, gid, maximum):
    before = _metadata(path, uid, gid)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        after = os.fstat(fd)
        if ((before.st_dev, before.st_ino, before.st_uid, before.st_gid, before.st_mode, before.st_nlink) !=
                (after.st_dev, after.st_ino, after.st_uid, after.st_gid, after.st_mode, after.st_nlink)):
            raise Refused('retention file changed during validation')
        if after.st_size > maximum:
            raise Refused('retention file exceeds its bound')
        result = os.read(fd, maximum + 1)
        if len(result) > maximum:
            raise Refused('retention file exceeds its bound')
        return result
    finally:
        os.close(fd)


def _sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Retention:
    def __init__(self, release, base=Path('/srv/weknora-production'),
                 data=Path('/data/weknora-production'), uid=0, gid=0):
        self.base, self.data = Path(base), Path(data)
        self.uid, self.gid = uid, gid
        supplied = Path(release)
        _no_symlinks(supplied)
        self.release = supplied.resolve(strict=True)
        if self.release.parent != self.base / 'releases':
            raise Refused('release must be an installed production release')
        self.root = self.data / 'retention'
        self.state_path = self.root / 'state.json'
        self.receipt_path = self.root / 'verified.json'
        self.key = self.root / 'key' / 'hmac.key'
        self.ledger = self.root / 'journal' / 'purge-ledger.json'
        self.pin = self.root / 'pin' / 'pin.json'
        self.compose = ['docker', 'compose', '--env-file', str(self.base / 'secrets/app.env'),
                        '--env-file', str(self.release / 'images.env'), '-f',
                        str(self.release / 'production/compose.yaml')]

    def protocol(self):
        try:
            manifest = json.loads((self.release / 'manifest.json').read_text())
            protocol = manifest.get('deployment_protocol', 1)
            features = manifest.get('required_features', [])
            if type(protocol) is not int or protocol not in (1, 2) or not isinstance(features, list):
                raise ValueError()
            if any(not isinstance(item, str) for item in features):
                raise ValueError()
            journal = 'original_body_journal' in features
            if (protocol == 2) != journal or (protocol == 1 and features):
                raise ValueError()
        except (OSError, ValueError, TypeError):
            raise Refused('invalid deployment journal contract') from None
        _no_symlinks(self.root)
        if not journal and self.root.exists() and any(self.root.iterdir()):
            raise Refused('legacy release cannot use an established retention anchor')
        return 'journal' if journal else 'legacy'

    def command(self, args, timeout=600):
        try:
            result = subprocess.run(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, timeout=timeout, check=False)
        except (OSError, subprocess.SubprocessError):
            raise Refused('offline retention command unavailable') from None
        if result.returncode:
            raise Refused('offline retention command refused; preserve database and anchor')
        return result.stdout

    def identity(self):
        try:
            lines = (self.release / 'images.env').read_text().splitlines()
            images = [line.removeprefix('APP_IMAGE=') for line in lines if line.startswith('APP_IMAGE=')]
            if len(images) != 1 or not re.fullmatch(r'sha256:[0-9a-f]{64}', images[0]):
                raise ValueError()
            output = self.command(['docker', 'run', '--rm', '--pull', 'never', '--network', 'none',
                                   '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
                                   '--pids-limit', '32', '--memory', '128m', '--entrypoint',
                                   '/usr/bin/python3', images[0], '-c', IDENTITY_COMMAND])
            result = json.loads(output)
            if (not isinstance(result, list) or len(result) != 2 or
                    any(type(value) is not int or value <= 0 for value in result)):
                raise ValueError()
            return tuple(result)
        except (OSError, ValueError, TypeError):
            raise Refused('image appuser identity is invalid') from None

    def readers_stopped(self):
        # Include restarting/paused containers, not only running ones.
        ids = self.command(self.compose + ['ps', '--all', '--quiet', 'app', 'frontend']).decode().split()
        for container in ids:
            if not re.fullmatch(r'[0-9a-f]{12,64}', container):
                raise Refused('invalid managed container identity')
            try:
                value = json.loads(self.command(['docker', 'inspect', '--format', '{{json .State}}', container]))
                if value.get('Running') or value.get('Restarting') or value.get('Paused'):
                    raise Refused('stop application and all readers before retention maintenance')
            except (ValueError, TypeError, AttributeError):
                raise Refused('cannot verify stopped application readers') from None

    def _mkdir(self, path, uid, gid):
        _no_symlinks(path)
        if not path.exists():
            path.mkdir(mode=0o700)
            os.chown(path, uid, gid)
            _sync_directory(path.parent)
        _metadata(path, uid, gid, directory=True)

    def _write_root_json(self, path, document):
        if path.exists():
            _metadata(path, self.uid, self.gid)
        temporary = self.root / ('.state-' + secrets.token_hex(12))
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            os.fchmod(fd, 0o600)
            os.fchown(fd, self.uid, self.gid)
            os.write(fd, json.dumps(document).encode())
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(temporary, path)
        _sync_directory(self.root)

    def _write_state(self, state, initial_schema=None):
        self._write_root_json(self.state_path, {'schema': 1, 'state': state, 'initial_schema': initial_schema})

    def _state(self):
        if not self.state_path.exists():
            return None
        try:
            state = json.loads(_read_private(self.state_path, self.uid, self.gid, 1024))
            if state.get('schema') != 1 or state.get('state') not in ('pending', 'initialized'):
                raise ValueError()
            if state['state'] == 'pending' and ('initial_schema' not in state or
                    (state['initial_schema'] is not None and
                     (type(state['initial_schema']) is not int or not 0 <= state['initial_schema'] < 152))):
                raise ValueError()
            return state
        except (ValueError, TypeError, AttributeError):
            raise Refused('retention provisioning state is invalid') from None

    def material(self, identity, create=False, initial_schema=None):
        uid, gid = identity
        _no_symlinks(self.root)
        if create:
            self._mkdir(self.root, self.uid, self.gid)
        _metadata(self.root, self.uid, self.gid, directory=True)
        for path in self.root.iterdir():
            if path.name in {'journal', 'key', 'pin', 'state.json', 'verified.json'}:
                continue
            if not re.fullmatch(r'\.state-[0-9a-f]{24}', path.name):
                raise Refused('unexpected retention material; operator inspection required')
            # A power loss during a root receipt/state atomic write can leave
            # this private file. It is never adopted as committed state.
            _metadata(path, self.uid, self.gid)
        record = self._state()
        state = record['state'] if record else None
        # Only an entirely empty root can authorize first initialization.
        pristine = state is None and not any(self.root.iterdir())
        if create and pristine:
            if initial_schema is not None and initial_schema >= 152:
                raise Refused('journal schema already exists; missing anchor cannot be initialized')
            self._write_state('pending', initial_schema)
            state = 'pending'
        for name, allowed in [('journal', {'purge-ledger.json', 'writer.lock'}), ('key', {'hmac.key'}), ('pin', {'pin.json'})]:
            directory = self.root / name
            if create and state == 'pending':
                self._mkdir(directory, uid, gid)
            _metadata(directory, uid, gid, directory=True)
            for path in directory.iterdir():
                # The CLI retains writer.lock and uses same-directory atomic
                # .body-journal-N files. A crash may leave a private temp file;
                # neither is anchor content and neither authorizes init/reset.
                temporary = name in ('journal', 'pin') and re.fullmatch(r'\.body-journal-[0-9]+', path.name)
                if path.name not in allowed and not temporary:
                    raise Refused('unexpected retention file; operator inspection required')
                _metadata(path, uid, gid)
        if not self.key.exists():
            # A missing key is recoverable only before ANY external anchor exists.
            if not create or state != 'pending' or self.ledger.exists() or self.pin.exists():
                raise Refused('retention key missing; initialization is forbidden')
            fd = os.open(self.key, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            try:
                os.fchmod(fd, 0o600)
                os.fchown(fd, uid, gid)
                os.write(fd, secrets.token_bytes(48))
                os.fsync(fd)
            finally:
                os.close(fd)
            _sync_directory(self.key.parent)
        if len(_read_private(self.key, uid, gid, 4096)) < 32:
            raise Refused('retention key too short')
        ledger, pin = self.ledger.exists(), self.pin.exists()
        if ledger != pin:
            raise Refused('partial retention anchor; explicit operator recovery required')
        if ledger:
            _metadata(self.ledger, uid, gid)
            _metadata(self.pin, uid, gid)
            return 'existing'
        if create and state == 'pending':
            return 'fresh'
        raise Refused('retention anchor missing; initialization is forbidden')

    def pre_migration_schema(self):
        # Unix-socket access inside the isolated PG container. No password is
        # passed in argv/environment and psql diagnostics are never forwarded.
        command = self.compose + ['exec', '-T', 'postgres', 'psql', '-X', '--no-password',
                                  '--username', 'weknora', '--dbname', 'weknora', '-Atc']
        exists = self.command(command + ["SELECT to_regclass('public.schema_migrations') IS NOT NULL"]).strip()
        if exists == b'f':
            return None
        if exists != b't':
            raise Refused('cannot inspect pre-migration database schema')
        value = self.command(command + ['SELECT version::text || \':\' || dirty::text FROM public.schema_migrations']).strip()
        match = re.fullmatch(rb'([0-9]+):(false|true)', value)
        if not match or match[2] != b'false':
            raise Refused('pre-migration database version is missing or dirty')
        return int(match[1])

    @staticmethod
    def boot_id():
        value = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        if not re.fullmatch(r'[0-9a-f-]{36}', value):
            raise Refused('cannot identify this boot for retention verification')
        return value

    def _app_image(self):
        values = [line[10:] for line in (self.release / 'images.env').read_text().splitlines()
                  if line.startswith('APP_IMAGE=')]
        if len(values) != 1 or not re.fullmatch(r'sha256:[0-9a-f]{64}', values[0]):
            raise Refused('invalid immutable application image')
        return values[0]

    def _receipt_identity(self, identity):
        try:
            journal_id = json.loads(_read_private(self.pin, *identity, 4096))['JournalID']
            if not isinstance(journal_id, str) or not re.fullmatch(r'[0-9a-f-]{36}', journal_id):
                raise ValueError()
        except (ValueError, KeyError, TypeError):
            raise Refused('invalid journal identity in retention pin') from None
        return {'release': str(self.release), 'image': self._app_image(), 'boot': self.boot_id(),
                'uid': identity[0], 'gid': identity[1], 'journal_id': journal_id,
                'key_sha256': hashlib.sha256(_read_private(self.key, *identity, 4096)).hexdigest()}

    def status(self, identity):
        self.material(identity)
        try:
            receipt = json.loads(_read_private(self.receipt_path, self.uid, self.gid, 4096))
            if (receipt.get('schema') != 1 or
                    any(receipt.get(key) != value for key, value in self._receipt_identity(identity).items()) or
                    type(receipt.get('verified_ns')) is not int):
                raise ValueError()
            ids = self.command(self.compose + ['ps', '--all', '--quiet', 'app']).decode().split()
            if len(ids) != 1 or not re.fullmatch(r'[0-9a-f]{12,64}', ids[0]):
                raise ValueError()
            container = json.loads(self.command(['docker', 'inspect', '--format', '{{json .}}', ids[0]]))
            state = container['State']
            if not state['Running'] or state.get('Restarting') or state.get('Paused') or state['Health']['Status'] != 'healthy':
                raise ValueError()
            # Python 3.10 accepts six fractional digits, Docker emits nine.
            started = re.sub(r'(\.\d{6})\d+(?=Z|[+-])', r'\1', state['StartedAt'])
            started_ns = int(datetime.fromisoformat(started.replace('Z', '+00:00')).timestamp() * 1e9)
            if started_ns < receipt['verified_ns'] or container['Image'] != receipt['image']:
                raise ValueError()
            policy = container['HostConfig']['RestartPolicy']
            if policy.get('Name') != 'on-failure' or policy.get('MaximumRetryCount') != 5:
                raise ValueError()
            expected_mounts = {str(self.root / 'journal'): ('/data/body-journal', True),
                               str(self.root / 'key'): ('/data/body-key', False),
                               str(self.root / 'pin'): ('/data/body-pin', True)}
            mounts = {item['Source']: (item['Destination'], item['RW']) for item in container['Mounts'] if item['Type'] == 'bind'}
            if any(mounts.get(source) != target for source, target in expected_mounts.items()):
                raise ValueError()
            expected_env = {'AUTO_MIGRATE': 'false', 'AUTO_RECOVER_DIRTY': 'false',
                            'WEKNORA_ORIGINAL_BODY_JOURNAL_DIRECTORY': '/data/body-journal',
                            'WEKNORA_ORIGINAL_BODY_JOURNAL_KEY': '/data/body-key/hmac.key',
                            'WEKNORA_ORIGINAL_BODY_JOURNAL_PIN': '/data/body-pin/pin.json'}
            entries = [item.split('=', 1) for item in container['Config']['Env']]
            if any(sum(item[0] == key for item in entries) != 1 for key in expected_env):
                raise ValueError()
            env = dict(entries)
            if any(env.get(key) != value for key, value in expected_env.items()):
                raise ValueError()
        except (ValueError, KeyError, TypeError, AttributeError):
            raise Refused('running application lacks this boot and release retention verification') from None
        return 'verified'

    def maintenance(self, mode, identity):
        self.readers_stopped()
        self.command(self.compose + ['run', '--rm', '--no-deps', '--pull', 'never', '-T',
                                    '--user', '%d:%d' % identity, '--entrypoint', '/usr/bin/python3',
                                    'app', '-c', CONTAINER_COMMAND, mode], timeout=1800)

    def run(self, mode):
        if self.protocol() != 'journal':
            if mode != 'protocol':
                raise Refused('release does not declare the journal maintenance protocol')
            return 'legacy'
        if mode == 'protocol':
            return 'journal'
        identity = self.identity()
        if mode == 'status':
            return self.status(identity)
        self.readers_stopped()
        version = self.pre_migration_schema() if mode == 'prepare' else None
        status = self.material(identity, create=(mode == 'prepare'), initial_schema=version)
        if self.receipt_path.exists():
            _metadata(self.receipt_path, self.uid, self.gid)
            self.receipt_path.unlink()
            _sync_directory(self.root)
        if mode == 'prepare':
            self.maintenance('migrate', identity)
            self.maintenance('init' if status == 'fresh' else 'verify', identity)
        elif mode == 'operator-reconcile':
            self.maintenance('reconcile', identity)
        elif mode != 'verify':
            raise Refused('unknown retention maintenance mode')
        self.maintenance('verify', identity)
        # Recheck newly written files before committing the root-only init latch.
        self.material(identity)
        if mode != 'verify':
            self._write_state('initialized')
        self._write_root_json(self.receipt_path, dict(schema=1, verified_ns=time.time_ns(),
                                                    **self._receipt_identity(identity)))
        return 'verified'


@contextlib.contextmanager
def deployment_lock(base, inherited=None):
    path = Path(base) / 'deploy.lock'
    _no_symlinks(path)
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600) if inherited is None else inherited
    try:
        value, expected = os.fstat(fd), path.lstat()
        if ((value.st_dev, value.st_ino) != (expected.st_dev, expected.st_ino) or
                not stat.S_ISREG(value.st_mode) or value.st_uid != 0 or
                value.st_nlink != 1 or stat.S_IMODE(value.st_mode) != 0o600):
            raise Refused('invalid production deployment lock')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise Refused('another deployment or retention maintenance holds the lock') from None
        yield
    finally:
        if inherited is None:
            os.close(fd)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('protocol', 'prepare', 'verify', 'status', 'operator-reconcile'))
    parser.add_argument('release', type=Path)
    parser.add_argument('--lock-fd', type=int)
    args = parser.parse_args()
    try:
        if os.geteuid() != 0:
            raise Refused('production retention maintenance requires root')
        manager = Retention(args.release)
        with deployment_lock(manager.base, args.lock_fd):
            result = manager.run(args.mode)
        print(result if args.mode == 'protocol' else ('Original body retention status verified.' if args.mode == 'status'
              else 'Original body retention verified; application remains stopped.'))
    except (Refused, OSError, ValueError):
        # Never stringify arbitrary exception details: Docker, paths or parsers may
        # include credentials. Refused messages are fixed constants from this file.
        error = sys.exc_info()[1]
        print(str(error) if isinstance(error, Refused) else 'Retention maintenance refused; preserve database and anchor.', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
