import json, os, subprocess

def image_id(name):
    return subprocess.check_output(['docker', 'image', 'inspect', name, '--format', '{{.Id}}'], text=True).strip()

if __name__ == '__main__':
    sha = os.environ['GITHUB_SHA']
    print(json.dumps({'schema': 1, 'commit': sha, 'tag': os.environ['GITHUB_REF_NAME'],
        'images': {name: image_id(ref) for name, ref in {
            'APP_IMAGE': 'weknora-production-app:' + sha,
            'UI_IMAGE': 'weknora-production-ui:' + sha,
            'POSTGRES_IMAGE': 'paradedb/paradedb:v0.22.6-pg17',
            'REDIS_IMAGE': 'redis:7.4.10-alpine',
        }.items()}}, indent=2))
