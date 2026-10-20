"""Reject malformed manifests before producing Compose environment values."""
import json, re, sys

def validate(m):
    if m.get('schema') != 1 or not re.fullmatch(r'[0-9a-f]{40}', m.get('commit', '')):
        raise ValueError('invalid schema or commit')
    if not re.fullmatch(r'prod-[A-Za-z0-9._-]+', m.get('tag', '')):
        raise ValueError('invalid production tag')
    protocol = m.get('deployment_protocol', 1)
    if type(protocol) is not int or protocol not in (1, 2):
        raise ValueError('unsupported deployment protocol')
    features = m.get('required_features', [])
    supported = {'original_body_journal', 'nextcloud_onboarding_v1', 'nextcloud_file_status_v2'}
    if not isinstance(features, list) or any(not isinstance(feature, str) or feature not in supported for feature in features):
        raise ValueError('unsupported required feature')
    if len(features) != len(set(features)):
        raise ValueError('duplicate required feature')
    if features and protocol != 2:
        raise ValueError('required features need deployment protocol 2')
    if protocol == 2 and 'original_body_journal' not in features:
        raise ValueError('protocol 2 requires an external original body journal')
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
