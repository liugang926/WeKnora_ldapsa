import copy
import hashlib
import json
import subprocess
import unittest
from contextlib import ExitStack, contextmanager
from unittest.mock import patch

from image_identity import ImageIdentityError, inspect_config_digest, resolve_images


KEYS = ['APP_IMAGE', 'UI_IMAGE', 'POSTGRES_IMAGE', 'REDIS_IMAGE']
COMMIT = 'a' * 40
SOCKET = '/run/containerd/containerd.sock'
IMAGE_TYPE = 'application/vnd.oci.image.manifest.v1+json'
INDEX_TYPE = 'application/vnd.oci.image.index.v1+json'
CONFIG_TYPE = 'application/vnd.oci.image.config.v1+json'
SECRET = 'do-not-print-environment-or-token'


def blob(value):
    content = json.dumps(value, sort_keys=True).encode()
    return 'sha256:' + hashlib.sha256(content).hexdigest(), content


def fixture(index=False):
    layers = ['sha256:' + '1' * 64, 'sha256:' + '2' * 64]
    config_id, config = blob({'architecture': 'amd64', 'os': 'linux',
                             'config': {'Env': ['TOKEN=' + SECRET]},
                             'rootfs': {'type': 'layers', 'diff_ids': layers}})
    manifest_id, manifest = blob({'schemaVersion': 2, 'mediaType': IMAGE_TYPE,
                                 'config': {'digest': config_id, 'size': len(config), 'mediaType': CONFIG_TYPE},
                                 'layers': []})
    contents = {config_id: config, manifest_id: manifest}
    descriptor = {'digest': manifest_id, 'size': len(manifest), 'mediaType': IMAGE_TYPE}
    if index:
        descriptor = dict(descriptor, platform={'os': 'linux', 'architecture': 'amd64'})
        index_id, index_blob = blob({'schemaVersion': 2, 'mediaType': INDEX_TYPE, 'manifests': [
            {'digest': 'sha256:' + 'f' * 64, 'size': 1, 'mediaType': IMAGE_TYPE,
             'platform': {'os': 'linux', 'architecture': 'amd64'},
             'annotations': {'vnd.docker.reference.type': 'attestation-manifest'}},
            descriptor,
        ]})
        contents[index_id] = index_blob
        descriptor = {'digest': index_id, 'size': len(index_blob), 'mediaType': INDEX_TYPE}
    info = {'Id': descriptor['digest'], 'Os': 'linux', 'Architecture': 'amd64',
            'RootFS': {'Type': 'layers', 'Layers': layers}, 'Descriptor': descriptor,
            'Config': {'Env': ['TOKEN=' + SECRET]}}
    return config_id, info, contents


def release(published):
    return {'schema': 1, 'commit': COMMIT, 'images': {key: published for key in KEYS}}


def fallback_infos(info):
    return {reference: info for reference in ['weknora-production-app:' + COMMIT,
                                             'weknora-production-ui:' + COMMIT,
                                             'paradedb/paradedb:v0.22.6-pg17',
                                             'redis:7.4.10-alpine']}


class ImageIdentityTests(unittest.TestCase):
    @contextmanager
    def runtime(self, infos, contents=None, *, sockets=None, uid=0, sudo_only=False):
        calls = []
        contents = contents or {}

        def run(command, **kwargs):
            calls.append(command)
            self.assertEqual(kwargs, {'capture_output': True, 'check': False, 'timeout': 30})
            if command[:3] == ['docker', 'image', 'inspect']:
                info = infos.get(command[3])
                if info is None:
                    return subprocess.CompletedProcess(command, 1, b'', SECRET.encode())
                return subprocess.CompletedProcess(command, 0, json.dumps([info]).encode(), b'')
            actual = command[2:] if command[:2] == ['sudo', '-n'] else command
            self.assertEqual(actual[0], 'ctr')
            self.assertEqual(actual[1:6], ['--address', SOCKET, '--namespace', 'moby', 'content'])
            self.assertEqual(actual[6], 'get')
            if sudo_only and command[0] != 'sudo':
                return subprocess.CompletedProcess(command, 1, b'', SECRET.encode())
            content = contents.get(actual[7])
            if content is None:
                return subprocess.CompletedProcess(command, 1, b'', SECRET.encode())
            return subprocess.CompletedProcess(command, 0, content, b'')

        with ExitStack() as stack:
            stack.enter_context(patch('image_identity._available_sockets', return_value=sockets if sockets is not None else [SOCKET]))
            stack.enter_context(patch('image_identity.os.geteuid', return_value=uid))
            stack.enter_context(patch('image_identity.subprocess.run', side_effect=run))
            yield calls

    def assert_sanitized_failure(self, action, expected):
        with self.assertRaisesRegex(ImageIdentityError, expected) as caught:
            action()
        self.assertNotIn(SECRET, str(caught.exception))

    def test_classic_published_config_ids_are_accepted_without_tag_or_ctr(self):
        config_id, info, _ = fixture()
        info.pop('Descriptor')
        info['Id'] = config_id
        manifest = release(config_id)
        original = copy.deepcopy(manifest)
        with self.runtime({config_id: info}, sockets=[]) as calls:
            result = resolve_images(manifest)
        self.assertEqual(manifest, original)
        self.assertEqual(result, {'schema': 1, 'published_images': manifest['images'],
                                  'local_images': manifest['images'], 'config_digests': manifest['images']})
        self.assertEqual(calls, [['docker', 'image', 'inspect', config_id]] * 4)

    def test_oci_store_tag_is_bound_to_published_config_and_layers(self):
        config_id, info, contents = fixture()
        manifest = release(config_id)
        with self.runtime(fallback_infos(info), contents) as calls:
            result = resolve_images(manifest)
        self.assertEqual(result['published_images'], manifest['images'])
        self.assertEqual(result['local_images'], {key: info['Id'] for key in KEYS})
        self.assertEqual(result['config_digests'], {key: config_id for key in KEYS})
        inspections = [command[3] for command in calls if command[0] == 'docker']
        self.assertEqual(inspections[::2], [config_id] * 4)
        self.assertCountEqual(inspections[1::2], list(fallback_infos(info)))

    def test_optional_discovery_reference_is_verified(self):
        config_id, info, contents = fixture()
        manifest = release(config_id)
        manifest['image_references'] = {key: 'release-image:' + key for key in KEYS}
        with self.runtime({value: info for value in manifest['image_references'].values()}, contents):
            result = resolve_images(manifest)
        self.assertEqual(result['config_digests'], {key: config_id for key in KEYS})

    def test_new_manifest_id_and_explicit_config_digest_are_supported(self):
        config_id, info, contents = fixture()
        manifest = release(info['Id'])
        manifest['image_config_digests'] = {key: config_id for key in KEYS}
        with self.runtime({info['Id']: info}, contents):
            result = resolve_images(manifest)
        self.assertEqual(result['local_images'], manifest['images'])
        self.assertEqual(result['config_digests'], manifest['image_config_digests'])

    def test_wrong_tag_config_is_rejected_even_with_valid_oci_blobs(self):
        _, info, contents = fixture()
        manifest = release('sha256:' + 'b' * 64)
        with self.runtime(fallback_infos(info), contents):
            self.assert_sanitized_failure(lambda: resolve_images(manifest), 'configuration digest mismatch')

    def test_explicit_config_digest_is_enforced_for_matching_local_id(self):
        _, info, contents = fixture()
        manifest = release(info['Id'])
        manifest['image_config_digests'] = {key: 'sha256:' + 'b' * 64 for key in KEYS}
        with self.runtime({info['Id']: info}, contents):
            self.assert_sanitized_failure(lambda: resolve_images(manifest), 'configuration digest mismatch')

    def test_changed_id_without_descriptor_is_rejected(self):
        config_id, info, _ = fixture()
        info.pop('Descriptor')
        with self.runtime(fallback_infos(info)):
            self.assert_sanitized_failure(lambda: resolve_images(release(config_id)), 'requires an OCI descriptor')

    def test_manifest_and_config_blob_sha_are_both_verified(self):
        config_id, info, contents = fixture()
        for identity in [info['Id'], config_id]:
            with self.subTest(identity=identity):
                broken = dict(contents)
                broken[identity] += b' '
                with self.runtime(fallback_infos(info), broken):
                    self.assert_sanitized_failure(lambda: resolve_images(release(config_id)), 'SHA-256 mismatch')

    def test_rootfs_layer_mismatch_is_rejected(self):
        config_id, info, contents = fixture()
        info['RootFS']['Layers'].reverse()
        with self.runtime(fallback_infos(info), contents):
            self.assert_sanitized_failure(lambda: resolve_images(release(config_id)), 'filesystem layers differ')

    def test_non_linux_or_non_amd64_docker_image_is_rejected(self):
        config_id, info, _ = fixture()
        for field, value in [('Os', 'windows'), ('Architecture', 'arm64')]:
            with self.subTest(field=field):
                wrong = copy.deepcopy(info)
                wrong[field] = value
                with self.runtime(fallback_infos(wrong)):
                    self.assert_sanitized_failure(lambda: resolve_images(release(config_id)), 'linux/amd64')

    def test_index_selects_one_linux_amd64_and_skips_attestation(self):
        config_id, info, contents = fixture(index=True)
        with self.runtime(fallback_infos(info), contents) as calls:
            result = resolve_images(release(config_id))
        self.assertEqual(result['config_digests'], {key: config_id for key in KEYS})
        self.assertFalse(any(command[-1] == 'sha256:' + 'f' * 64 for command in calls))

    def test_ambiguous_index_is_rejected(self):
        config_id, info, contents = fixture(index=True)
        index = json.loads(contents[info['Id']])
        index['manifests'].append(index['manifests'][-1])
        identity, content = blob(index)
        contents[identity] = content
        info['Id'] = identity
        info['Descriptor'] = {'digest': identity, 'size': len(content), 'mediaType': INDEX_TYPE}
        with self.runtime(fallback_infos(info), contents):
            self.assert_sanitized_failure(lambda: resolve_images(release(config_id)), 'exactly one linux/amd64')

    def test_nonroot_ctr_can_fall_back_to_noninteractive_sudo(self):
        config_id, info, contents = fixture()
        with self.runtime({'some-image:tag': info}, contents, uid=1001, sudo_only=True) as calls:
            self.assertEqual(inspect_config_digest('some-image:tag'), config_id)
        content_calls = [command for command in calls if command[0] != 'docker']
        self.assertEqual(len(content_calls), 4)
        self.assertEqual([command[0] for command in content_calls], ['ctr', 'sudo', 'ctr', 'sudo'])

    def test_unavailable_content_fails_closed_without_daemon_diagnostics(self):
        config_id, info, _ = fixture()
        with self.runtime(fallback_infos(info)):
            self.assert_sanitized_failure(lambda: resolve_images(release(config_id)), 'content is unavailable')

    def test_classic_inspect_config_digest_returns_config_id(self):
        config_id, info, _ = fixture()
        info.pop('Descriptor')
        info['Id'] = config_id
        with self.runtime({'some-image:tag': info}, sockets=[]):
            self.assertEqual(inspect_config_digest('some-image:tag'), config_id)

    def test_discovery_reference_cannot_be_a_cli_option(self):
        config_id, info, _ = fixture()
        manifest = release(config_id)
        manifest['image_references'] = {'APP_IMAGE': '--format=' + SECRET}
        with self.runtime({}):
            self.assert_sanitized_failure(lambda: resolve_images(manifest), 'Invalid image discovery reference')


if __name__ == '__main__':
    unittest.main()
