"""Resolve release image identities across Docker's classic and OCI stores.

Tags are only discovery hints. Every resolved image is bound to an immutable
published identity or to its verified OCI configuration and filesystem layers.
"""

import hashlib
import json
import os
import pathlib
import re
import subprocess


_SOCKETS = ('/run/containerd/containerd.sock', '/run/docker/containerd/containerd.sock')
_IMAGE_TYPES = {
    'application/vnd.oci.image.manifest.v1+json',
    'application/vnd.docker.distribution.manifest.v2+json',
}
_INDEX_TYPES = {
    'application/vnd.oci.image.index.v1+json',
    'application/vnd.docker.distribution.manifest.list.v2+json',
}
_CONFIG_TYPES = {
    'application/vnd.oci.image.config.v1+json',
    'application/vnd.docker.container.image.v1+json',
}
_KEYS = {'APP_IMAGE', 'UI_IMAGE', 'POSTGRES_IMAGE', 'REDIS_IMAGE'}


class ImageIdentityError(RuntimeError):
    """A deliberately sanitized identity error, with no image environment data."""


def _digest(value):
    if not isinstance(value, str) or re.fullmatch(r'sha256:[0-9a-f]{64}', value) is None:
        raise ImageIdentityError('Invalid SHA-256 image identity')
    return value


def _reference(value):
    if not isinstance(value, str) or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:/@-]{0,511}', value) is None:
        raise ImageIdentityError('Invalid image discovery reference')
    return value


def _json(blob, description):
    try:
        value = json.loads(blob)
    except (ValueError, TypeError, UnicodeDecodeError):
        raise ImageIdentityError('Invalid ' + description + ' JSON') from None
    if not isinstance(value, dict):
        raise ImageIdentityError('Invalid ' + description + ' object')
    return value


def _run(command):
    try:
        return subprocess.run(command, capture_output=True, check=False, timeout=30)
    except (OSError, subprocess.SubprocessError):
        # Captured Docker output can contain Env credentials; ctr failures can
        # contain arbitrary daemon diagnostics. Never include either in errors.
        return None


def _inspect(reference):
    result = _run(['docker', 'image', 'inspect', _reference(reference)])
    if result is None or result.returncode != 0:
        return None
    try:
        values = json.loads(result.stdout)
    except (ValueError, TypeError, UnicodeDecodeError):
        raise ImageIdentityError('Invalid Docker inspection JSON') from None
    if not isinstance(values, list) or len(values) != 1 or not isinstance(values[0], dict):
        raise ImageIdentityError('Unexpected Docker inspection result')
    return values[0]


def _platform(info):
    if info.get('Os') != 'linux' or info.get('Architecture') != 'amd64':
        raise ImageIdentityError('Docker image must be linux/amd64')
    rootfs = info.get('RootFS')
    if not isinstance(rootfs, dict) or rootfs.get('Type') != 'layers':
        raise ImageIdentityError('Docker image has no verifiable layered filesystem')
    layers = rootfs.get('Layers')
    if not isinstance(layers, list):
        raise ImageIdentityError('Invalid Docker filesystem layers')
    return [_digest(layer) for layer in layers]


def _available_sockets():
    return [path for path in _SOCKETS if pathlib.Path(path).is_socket()]


def _content(digest):
    digest = _digest(digest)
    for address in _available_sockets():
        command = ['ctr', '--address', address, '--namespace', 'moby', 'content', 'get', digest]
        commands = [command]
        if os.geteuid() != 0:
            commands.append(['sudo', '-n'] + command)
        for candidate in commands:
            result = _run(candidate)
            if result is None or result.returncode != 0:
                continue
            blob = result.stdout
            if not isinstance(blob, bytes):
                raise ImageIdentityError('Containerd content must be binary data')
            if 'sha256:' + hashlib.sha256(blob).hexdigest() != digest:
                raise ImageIdentityError('Containerd content SHA-256 mismatch')
            return blob
    raise ImageIdentityError('Verified containerd content is unavailable')


def _descriptor_blob(descriptor):
    if not isinstance(descriptor, dict):
        raise ImageIdentityError('Missing OCI descriptor')
    digest = _digest(descriptor.get('digest'))
    blob = _content(digest)
    size = descriptor.get('size')
    if size is not None and (not isinstance(size, int) or size < 0 or len(blob) != size):
        raise ImageIdentityError('OCI descriptor size mismatch')
    return digest, blob


def _oci_config(info, layers):
    descriptor = info.get('Descriptor')
    if not isinstance(descriptor, dict):
        raise ImageIdentityError('Different local image identity requires an OCI descriptor')
    identities = set()
    for _ in range(5):
        digest, blob = _descriptor_blob(descriptor)
        if digest in identities:
            raise ImageIdentityError('Recursive OCI descriptor')
        identities.add(digest)
        document = _json(blob, 'OCI descriptor')
        if document.get('schemaVersion') != 2:
            raise ImageIdentityError('Unsupported OCI schema')
        media_type = descriptor.get('mediaType') or document.get('mediaType')
        if media_type in _INDEX_TYPES:
            manifests = document.get('manifests')
            if not isinstance(manifests, list):
                raise ImageIdentityError('Invalid OCI index')
            candidates = []
            for entry in manifests:
                if not isinstance(entry, dict):
                    continue
                annotations = entry.get('annotations', {})
                if not isinstance(annotations, dict):
                    raise ImageIdentityError('Invalid OCI index annotations')
                if annotations.get('vnd.docker.reference.type') == 'attestation-manifest':
                    continue
                platform = entry.get('platform', {})
                if isinstance(platform, dict) and platform.get('os') == 'linux' and platform.get('architecture') == 'amd64':
                    candidates.append(entry)
            if len(candidates) != 1:
                raise ImageIdentityError('OCI index must contain exactly one linux/amd64 image')
            descriptor = candidates[0]
            continue
        if media_type not in _IMAGE_TYPES:
            raise ImageIdentityError('Unsupported OCI image media type')
        config_descriptor = document.get('config')
        if not isinstance(config_descriptor, dict) or config_descriptor.get('mediaType') not in _CONFIG_TYPES:
            raise ImageIdentityError('Invalid OCI image config descriptor')
        config_digest, config_blob = _descriptor_blob(config_descriptor)
        identities.add(config_digest)
        config = _json(config_blob, 'OCI image config')
        if config.get('os') != 'linux' or config.get('architecture') != 'amd64':
            raise ImageIdentityError('OCI image config must be linux/amd64')
        rootfs = config.get('rootfs')
        if not isinstance(rootfs, dict) or rootfs.get('type') != 'layers' or rootfs.get('diff_ids') != layers:
            raise ImageIdentityError('OCI config and Docker filesystem layers differ')
        if _digest(info.get('Id')) not in identities:
            raise ImageIdentityError('Docker identity is not bound to the OCI descriptor')
        return config_digest
    raise ImageIdentityError('OCI index nesting limit exceeded')


def _config_digest(info, require_descriptor=False):
    layers = _platform(info)
    local_id = _digest(info.get('Id'))
    if isinstance(info.get('Descriptor'), dict):
        return _oci_config(info, layers)
    if require_descriptor:
        raise ImageIdentityError('Different local image identity requires an OCI descriptor')
    # In Docker's classic store, Id is the image configuration digest itself.
    return local_id


def inspect_config_digest(reference):
    """Return the verified real configuration digest for one local image."""
    info = _inspect(reference)
    if info is None:
        raise ImageIdentityError('Image inspection failed')
    return _config_digest(info)


def resolve_images(manifest):
    """Resolve immutable published identities to this daemon's exact image IDs."""
    if not isinstance(manifest, dict) or manifest.get('schema') != 1:
        raise ImageIdentityError('Unsupported release schema')
    published = manifest.get('images')
    commit = manifest.get('commit')
    if not isinstance(published, dict) or set(published) != _KEYS:
        raise ImageIdentityError('Incomplete published image set')
    if not isinstance(commit, str) or re.fullmatch(r'[0-9a-f]{40}', commit) is None:
        raise ImageIdentityError('Invalid release commit')
    references = manifest.get('image_references', {})
    expected_configs = manifest.get('image_config_digests', {})
    if not isinstance(references, dict) or not isinstance(expected_configs, dict):
        raise ImageIdentityError('Invalid release image identity metadata')
    if set(references) - _KEYS or set(expected_configs) - _KEYS:
        raise ImageIdentityError('Unknown release image identity key')
    fallback = {
        'APP_IMAGE': 'weknora-production-app:' + commit,
        'UI_IMAGE': 'weknora-production-ui:' + commit,
        'POSTGRES_IMAGE': 'paradedb/paradedb:v0.22.6-pg17',
        'REDIS_IMAGE': 'redis:7.4.10-alpine',
    }
    local_images = {}
    config_digests = {}
    for key in sorted(_KEYS):
        published_id = _digest(published[key])
        expected_config = _digest(expected_configs.get(key, published_id))
        info = _inspect(published_id)
        if info is None:
            info = _inspect(references.get(key, fallback[key]))
        if info is None:
            raise ImageIdentityError('Published image is not available locally: ' + key)
        local_id = _digest(info.get('Id'))
        config_digest = _config_digest(info, require_descriptor=local_id != published_id)
        if (local_id != published_id or key in expected_configs) and config_digest != expected_config:
            raise ImageIdentityError('Published configuration digest mismatch: ' + key)
        local_images[key] = local_id
        config_digests[key] = config_digest
    return {'schema': 1, 'published_images': dict(published),
            'local_images': local_images, 'config_digests': config_digests}
