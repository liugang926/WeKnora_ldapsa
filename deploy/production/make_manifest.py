import json, os, subprocess

def image_id(name):
    return subprocess.check_output(['docker', 'image', 'inspect', name, '--format', '{{.Id}}'], text=True).strip()

if __name__ == '__main__':
    from image_identity import inspect_config_digest
    sha = os.environ['GITHUB_SHA']
    tag = os.environ.get('RELEASE_TAG') or os.environ['GITHUB_REF_NAME']
    refs = {'APP_IMAGE': 'weknora-production-app:' + sha,
            'UI_IMAGE': 'weknora-production-ui:' + sha,
            'POSTGRES_IMAGE': 'paradedb/paradedb:v0.22.6-pg17',
            'REDIS_IMAGE': 'redis:7.4.10-alpine'}
    print(json.dumps({'schema': 1, 'commit': sha, 'tag': tag,
        'images': {key: image_id(ref) for key, ref in refs.items()},
        'image_references': refs,
        'image_config_digests': {key: inspect_config_digest(ref) for key, ref in refs.items()}}, indent=2))
