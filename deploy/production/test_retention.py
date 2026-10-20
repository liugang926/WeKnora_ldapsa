import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest
from unittest import mock
from urllib.parse import unquote, urlsplit

import retention


IMAGE = 'sha256:' + 'b' * 64
JOURNAL_ID = '11111111-2222-4333-8444-555555555555'
BOOT_ID = 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee'


class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name).resolve()
        self.base, self.data = root / 'srv', root / 'data'
        self.release = self.base / 'releases/prod-test'
        self.release.mkdir(parents=True)
        self.data.mkdir()
        self.manifest = {'schema': 1, 'deployment_protocol': 2,
                         'required_features': ['original_body_journal', 'nextcloud_onboarding_v1']}
        self.write_manifest()
        (self.release / 'images.env').write_text('APP_IMAGE=' + IMAGE + '\n')
        self.uid, self.gid = os.getuid(), os.getgid()
        self.app_uid = self.uid if self.uid else 1000
        self.app_gid = self.gid if self.uid else 1000
        self.identity = self.app_uid, self.app_gid
        self.manager = retention.Retention(self.release, self.base, self.data, self.uid, self.gid)
        self.manager.identity = mock.Mock(return_value=self.identity)
        self.manager.readers_stopped = mock.Mock()
        self.manager.pre_migration_schema = mock.Mock(return_value=132)
        self.manager.boot_id = mock.Mock(return_value=BOOT_ID)
        self.modes = []

        def maintenance(mode, identity):
            self.modes.append(mode)
            if mode == 'init':
                self.private(self.manager.ledger, b'[]')
                self.private(self.manager.pin, json.dumps({'JournalID': JOURNAL_ID}).encode())
        self.manager.maintenance = mock.Mock(side_effect=maintenance)

    def write_manifest(self):
        (self.release / 'manifest.json').write_text(json.dumps(self.manifest))

    def private(self, path, content):
        path.write_bytes(content)
        path.chmod(0o600)
        if os.getuid() == 0:
            os.chown(path, self.app_uid, self.app_gid)

    def prepared(self):
        self.assertEqual(self.manager.run('prepare'), 'verified')
        self.modes.clear()
        self.manager.maintenance.reset_mock()

    def test_first_upgrade_migrates_initializes_then_verifies(self):
        self.manager.run('prepare')
        self.assertEqual(self.modes, ['migrate', 'init', 'verify'])
        self.manager.pre_migration_schema.assert_called_once()
        self.assertEqual(self.manager._state()['state'], 'initialized')
        self.assertEqual(len(self.manager.key.read_bytes()), 48)
        self.assertEqual(stat.S_IMODE(self.manager.key.stat().st_mode), 0o600)
        for name in ('journal', 'key', 'pin'):
            self.assertEqual(stat.S_IMODE((self.manager.root / name).stat().st_mode), 0o700)
            self.assertEqual((self.manager.root / name).stat().st_uid, self.app_uid)
        receipt = json.loads(self.manager.receipt_path.read_text())
        self.assertEqual(receipt['image'], IMAGE)
        self.assertEqual(receipt['boot'], BOOT_ID)
        self.assertNotIn(self.manager.key.read_bytes().hex(), self.manager.receipt_path.read_text())

    def test_fresh_database_can_initialize(self):
        self.manager.pre_migration_schema.return_value = None
        self.manager.run('prepare')
        self.assertEqual(self.modes, ['migrate', 'init', 'verify'])

    def test_existing_anchor_is_never_reinitialized_or_rekeyed(self):
        self.prepared()
        key = self.manager.key.read_bytes()
        self.manager.pre_migration_schema.return_value = 164
        self.manager.run('prepare')
        self.assertEqual(self.modes, ['migrate', 'verify', 'verify'])
        self.assertEqual(self.manager.key.read_bytes(), key)

    def test_schema_at_or_after_journal_introduction_cannot_initialize_missing_anchor(self):
        for version in (152, 164):
            self.manager.pre_migration_schema.return_value = version
            with self.subTest(version=version), self.assertRaises(retention.Refused):
                self.manager.run('prepare')
            self.assertFalse(self.manager.key.exists())
            self.assertFalse(self.modes)

    def test_boot_verify_never_migrates_initializes_or_reconciles(self):
        self.prepared()
        self.manager.run('verify')
        self.assertEqual(self.modes, ['verify'])
        self.assertEqual(self.manager.pre_migration_schema.call_count, 1)

    def test_real_writer_lock_and_private_atomic_leftovers_do_not_block_recovery(self):
        self.prepared()
        self.private(self.manager.root / 'journal/writer.lock', b'')
        self.private(self.manager.root / 'journal/.body-journal-1234567', b'[]')
        self.private(self.manager.root / 'pin/.body-journal-7654321', b'{}')
        self.manager.run('verify')
        self.assertEqual(self.modes, ['verify'])
        self.assertEqual(self.status(), 'verified')

    def test_unsafe_writer_lock_is_refused(self):
        self.prepared()
        lock = self.manager.root / 'journal/writer.lock'
        self.private(lock, b'')
        lock.chmod(0o644)
        with self.assertRaises(retention.Refused):
            self.manager.run('verify')
        self.assertFalse(self.modes)

    def test_interrupted_root_receipt_temp_is_not_adopted_and_does_not_block_boot(self):
        self.prepared()
        temporary = self.manager.root / ('.state-' + 'a' * 24)
        temporary.write_text('{incomplete')
        temporary.chmod(0o600)
        self.manager.run('verify')
        self.assertEqual(self.modes, ['verify'])
        self.assertEqual(self.status(), 'verified')
        self.assertEqual(temporary.read_text(), '{incomplete')

    def test_boot_missing_anchor_does_not_create_anything(self):
        with self.assertRaises(retention.Refused):
            self.manager.run('verify')
        self.assertFalse(self.manager.root.exists())
        self.assertFalse(self.modes)

    def test_partial_anchor_fails_without_removal_or_regeneration(self):
        self.prepared()
        self.manager.pin.unlink()
        key = self.manager.key.read_bytes()
        with self.assertRaises(retention.Refused):
            self.manager.run('prepare')
        self.assertEqual(self.manager.key.read_bytes(), key)
        self.assertTrue(self.manager.ledger.exists())
        self.assertFalse(self.manager.pin.exists())
        self.assertFalse(self.modes)

    def test_initialized_latch_prevents_reinitialization_after_both_anchor_files_disappear(self):
        self.prepared()
        self.manager.ledger.unlink()
        self.manager.pin.unlink()
        with self.assertRaises(retention.Refused):
            self.manager.run('prepare')
        self.assertFalse(self.modes)

    def test_missing_key_with_anchor_is_never_regenerated(self):
        self.prepared()
        self.manager.key.unlink()
        with self.assertRaises(retention.Refused):
            self.manager.run('prepare')
        self.assertFalse(self.manager.key.exists())
        self.assertFalse(self.modes)

    def test_unlatched_key_only_cannot_authorize_initialization(self):
        self.manager.material(self.identity, create=True, initial_schema=132)
        self.manager.state_path.unlink()
        with self.assertRaises(retention.Refused):
            self.manager.run('prepare')
        self.assertFalse(self.modes)

    def test_interrupted_first_migration_keeps_authorized_pending_latch(self):
        self.manager.material(self.identity, create=True, initial_schema=132)
        self.manager.pre_migration_schema.return_value = 164
        self.manager.run('prepare')
        self.assertEqual(self.modes, ['migrate', 'init', 'verify'])

    def test_migration_failure_preserves_key_and_pending_latch(self):
        self.manager.maintenance.side_effect = retention.Refused('offline retention command refused')
        with self.assertRaises(retention.Refused):
            self.manager.run('prepare')
        self.assertTrue(self.manager.key.exists())
        self.assertEqual(self.manager._state()['state'], 'pending')
        self.assertFalse(self.manager.receipt_path.exists())
        self.assertEqual(self.manager.maintenance.call_args.args[0], 'migrate')

    def test_failed_verify_removes_previous_receipt_without_implicit_reconcile(self):
        self.prepared()
        self.manager.maintenance.side_effect = retention.Refused('offline retention command refused')
        with self.assertRaises(retention.Refused):
            self.manager.run('verify')
        self.assertEqual(self.manager.maintenance.call_args.args[0], 'verify')
        self.assertFalse(self.manager.receipt_path.exists())

    def test_explicit_reconcile_requires_existing_anchor_then_verifies(self):
        self.prepared()
        marker = self.base / 'ACTIVATION_PENDING'
        marker.write_text('operator recovery')
        self.manager.run('operator-reconcile')
        self.assertEqual(self.modes, ['reconcile', 'verify'])
        self.assertEqual(marker.read_text(), 'operator recovery')

    def test_failed_reconcile_never_gets_verification_receipt(self):
        self.prepared()
        self.manager.maintenance.side_effect = retention.Refused('offline retention command refused')
        with self.assertRaises(retention.Refused):
            self.manager.run('operator-reconcile')
        self.assertFalse(self.manager.receipt_path.exists())
        self.assertEqual(self.manager.maintenance.call_args.args[0], 'reconcile')

    def test_wrong_file_permission_and_multiple_hardlinks_refused(self):
        self.prepared()
        self.manager.key.chmod(0o644)
        with self.assertRaises(retention.Refused):
            self.manager.run('verify')
        self.manager.key.chmod(0o600)
        extra = self.data / 'linked-key'
        os.link(self.manager.key, extra)
        with self.assertRaises(retention.Refused):
            self.manager.run('verify')
        self.assertEqual(self.manager.key.stat().st_nlink, 2)

    def test_wrong_owner_refused_without_chown_fix(self):
        self.prepared()
        with mock.patch.object(self.manager, 'identity', return_value=(self.app_uid + 1, self.app_gid)):
            with self.assertRaises(retention.Refused):
                self.manager.run('prepare')
        self.assertEqual(self.manager.key.stat().st_uid, self.app_uid)
        self.assertFalse(self.modes)

    def test_symlink_file_and_symlink_ancestor_refused(self):
        self.prepared()
        saved = self.data / 'saved-key'
        self.manager.key.rename(saved)
        self.manager.key.symlink_to(saved)
        with self.assertRaises(retention.Refused):
            self.manager.run('verify')
        self.manager.key.unlink()
        saved.rename(self.manager.key)
        directory = self.manager.root / 'pin'
        saved_directory = self.data / 'saved-pin'
        directory.rename(saved_directory)
        directory.symlink_to(saved_directory, target_is_directory=True)
        with self.assertRaises(retention.Refused):
            self.manager.run('verify')

    def test_legacy_release_refused_after_anchor_exists(self):
        self.prepared()
        self.manifest.pop('deployment_protocol')
        self.manifest.pop('required_features')
        self.write_manifest()
        with self.assertRaises(retention.Refused):
            self.manager.protocol()

    def test_legacy_release_allowed_before_first_anchor(self):
        self.manifest = {'schema': 1}
        self.write_manifest()
        self.assertEqual(self.manager.run('protocol'), 'legacy')
        self.assertFalse(self.manager.root.exists())

    def test_inconsistent_journal_contract_refused(self):
        for document in ({'deployment_protocol': 2}, {'required_features': ['original_body_journal']},
                         {'deployment_protocol': True}, {'deployment_protocol': 3}):
            self.manifest = document
            self.write_manifest()
            with self.subTest(document=document), self.assertRaises(retention.Refused):
                self.manager.protocol()

    def test_subprocess_diagnostics_never_leak_secrets_or_urls(self):
        secret = 'credential-secret https://example.invalid/signed?token=secret'
        with mock.patch('retention.subprocess.run', return_value=subprocess.CompletedProcess([], 1, b'', secret.encode())):
            with self.assertRaises(retention.Refused) as caught:
                self.manager.command(['docker', 'compose'])
        self.assertNotIn('credential', str(caught.exception))
        self.assertNotIn('https:', str(caught.exception))

    def test_timeout_is_sanitized(self):
        with mock.patch('retention.subprocess.run', side_effect=subprocess.TimeoutExpired('secret-value', 1)):
            with self.assertRaises(retention.Refused) as caught:
                self.manager.command(['docker'])
        self.assertNotIn('secret-value', str(caught.exception))

    def test_reader_check_rejects_running_restarting_or_paused(self):
        with mock.patch.object(self.manager, 'command') as command:
            for flag in ('Running', 'Restarting', 'Paused'):
                command.side_effect = [b'a' * 64, json.dumps({flag: True}).encode()]
                with self.subTest(flag=flag), self.assertRaises(retention.Refused):
                    retention.Retention.readers_stopped(self.manager)

    def test_maintenance_passes_no_dsn_credentials_or_user_zero(self):
        with mock.patch.object(self.manager, 'command', return_value=b'') as command:
            retention.Retention.maintenance(self.manager, 'verify', self.identity)
        args = command.call_args.args[0]
        self.assertIn('--no-deps', args)
        self.assertEqual(args[args.index('--user') + 1], '%d:%d' % self.identity)
        self.assertEqual(args[-1], 'verify')
        self.assertIn(CONTAINER_COMMAND := retention.CONTAINER_COMMAND, args)
        self.assertNotIn('postgres://weknora:', ' '.join(args))
        self.assertIn("quote(password, safe='')", CONTAINER_COMMAND)
        self.assertIn('os.dup2(null, 2)', CONTAINER_COMMAND)

    def test_pre_migration_dirty_version_refused(self):
        with mock.patch.object(self.manager, 'command', side_effect=[b't\n', b'132:true\n']):
            with self.assertRaises(retention.Refused):
                retention.Retention.pre_migration_schema(self.manager)

    def test_container_wrapper_constructs_url_encoded_dsn_in_environment_only(self):
        environment = {'DB_DRIVER': 'postgres', 'DB_HOST': 'postgres', 'DB_PORT': '5432',
                       'DB_USER': 'u@ser', 'DB_PASSWORD': 'private@:/?#% value', 'DB_NAME': 'db/name'}
        with mock.patch.dict(os.environ, environment, clear=True), \
                mock.patch('sys.argv', ['wrapper', 'verify']), \
                mock.patch('os.open', return_value=99), mock.patch('os.close'), mock.patch('os.dup2'), \
                mock.patch('os.execv') as execute:
            exec(retention.CONTAINER_COMMAND, {})
            parsed = urlsplit(os.environ['WEKNORA_ORIGINAL_BODY_MAINTENANCE_DSN'])
            self.assertEqual(unquote(parsed.username), environment['DB_USER'])
            self.assertEqual(unquote(parsed.password), environment['DB_PASSWORD'])
            self.assertEqual(unquote(parsed.path[1:]), environment['DB_NAME'])
            self.assertEqual(execute.call_args.args,
                             ('/usr/local/bin/original-body-retention',
                              ['original-body-retention', '-driver', 'postgres', '-mode', 'verify']))
            self.assertNotIn(environment['DB_PASSWORD'], str(execute.call_args))

    def test_pre_migration_clean_and_empty_database(self):
        with mock.patch.object(self.manager, 'command', side_effect=[b't\n', b'132:false\n']):
            self.assertEqual(retention.Retention.pre_migration_schema(self.manager), 132)
        with mock.patch.object(self.manager, 'command', return_value=b'f\n'):
            self.assertIsNone(retention.Retention.pre_migration_schema(self.manager))

    def running_container(self):
        return {'Image': IMAGE, 'State': {'Running': True, 'Health': {'Status': 'healthy'},
                'StartedAt': '2099-01-01T00:00:00.000000000Z'},
                'HostConfig': {'RestartPolicy': {'Name': 'on-failure', 'MaximumRetryCount': 5}},
                'Config': {'Env': ['AUTO_MIGRATE=false', 'AUTO_RECOVER_DIRTY=false',
                                   'WEKNORA_ORIGINAL_BODY_JOURNAL_DIRECTORY=/data/body-journal',
                                   'WEKNORA_ORIGINAL_BODY_JOURNAL_KEY=/data/body-key/hmac.key',
                                   'WEKNORA_ORIGINAL_BODY_JOURNAL_PIN=/data/body-pin/pin.json']},
                'Mounts': [{'Type': 'bind', 'Source': str(self.manager.root / name),
                            'Destination': '/data/body-' + name, 'RW': name != 'key'}
                           for name in ('journal', 'key', 'pin')]}

    def status(self, container=None):
        container = self.running_container() if container is None else container
        with mock.patch.object(self.manager, 'command', side_effect=[b'a' * 64, json.dumps(container).encode()]):
            return self.manager.run('status')

    def test_running_status_uses_receipt_without_offline_cli(self):
        self.prepared()
        before = self.manager.receipt_path.read_bytes()
        self.assertEqual(self.status(), 'verified')
        self.manager.maintenance.assert_not_called()
        self.assertEqual(self.manager.receipt_path.read_bytes(), before)

    def test_status_rejects_previous_boot_receipt(self):
        self.prepared()
        self.manager.boot_id.return_value = 'bbbbbbbb-bbbb-4ccc-8ddd-eeeeeeeeeeee'
        with self.assertRaises(retention.Refused):
            self.status()

    def test_status_rejects_wrong_image_mount_environment_or_old_start(self):
        self.prepared()
        for field in ('image', 'mount', 'env', 'start', 'restart'):
            container = self.running_container()
            if field == 'image': container['Image'] = 'sha256:' + 'c' * 64
            if field == 'mount': container['Mounts'][1]['RW'] = True
            if field == 'env': container['Config']['Env'] = []
            if field == 'start': container['State']['StartedAt'] = '2000-01-01T00:00:00Z'
            if field == 'restart': container['HostConfig']['RestartPolicy']['Name'] = 'unless-stopped'
            with self.subTest(field=field), self.assertRaises(retention.Refused):
                self.status(container)

    def test_status_rejects_replaced_key_even_with_correct_permissions(self):
        self.prepared()
        self.private(self.manager.key, b'z' * 48)
        with self.assertRaises(retention.Refused):
            self.status()


if __name__ == '__main__':
    unittest.main()
