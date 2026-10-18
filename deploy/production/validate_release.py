"""Reject malformed manifests before producing Compose environment values."""
import json, re, sys

def validate(m):
    if m.get('schema') != 1 or not re.fullmatch(r'[0-9a-f]{40}', m.get('commit', '')):
        raise ValueError('invalid schema or commit')
    if not re.fullmatch(r'prod-[A-Za-z0-9._-]+', m.get('tag', '')):
        raise ValueError('invalid production tag')
    if set(m.get('images', {})) != {'APP_IMAGE', 'UI_IMAGE', 'POSTGRES_IMAGE', 'REDIS_IMAGE'}:
        raise ValueError('incomplete image set')
    for value in m['images'].values():
        if not re.fullmatch(r'sha256:[0-9a-f]{64}', value):
            raise ValueError('images must use immutable local IDs')
    return m

if __name__ == '__main__':
    m = validate(json.load(open(sys.argv[1])))
    for key, value in sorted(m['images'].items()):
        print(key + '=' + value)
