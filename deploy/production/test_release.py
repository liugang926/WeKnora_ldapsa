import copy, unittest
from validate_release import validate
class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.m = {'schema': 1, 'commit': 'a'*40, 'tag': 'prod-20261018.1',
          'images': {k: 'sha256:'+'b'*64 for k in ['APP_IMAGE','UI_IMAGE','POSTGRES_IMAGE','REDIS_IMAGE']}}
    def test_valid(self):
        self.assertEqual(validate(self.m), self.m)
    def test_retention_release_declares_protocol_and_journal(self):
        self.m.update(deployment_protocol=2, required_features=['original_body_journal', 'nextcloud_onboarding_v1', 'nextcloud_file_status_v2'])
        self.assertEqual(validate(self.m), self.m)
    def test_unknown_protocol_or_required_feature_refuses_activation(self):
        for protocol in [True, 0, 3, '2']:
            m = dict(self.m, deployment_protocol=protocol)
            with self.subTest(protocol=protocol), self.assertRaises(ValueError): validate(m)
        for features in [['unreviewed_feature'], 'original_body_journal', [None], ['original_body_journal']]:
            m = dict(self.m, required_features=features)
            with self.subTest(features=features), self.assertRaises(ValueError): validate(m)
        for features in [[], ['nextcloud_onboarding_v1'], ['original_body_journal', 'original_body_journal']]:
            m = dict(self.m, deployment_protocol=2, required_features=features)
            with self.subTest(features=features), self.assertRaises(ValueError): validate(m)
    def test_missing_dependency(self):
        del self.m['images']['REDIS_IMAGE']
        with self.assertRaises(ValueError): validate(self.m)
    def test_mutable_or_injected_images(self):
        for value in ['redis:latest', 'sha256:'+'b'*64+'\nHTTP_PORT=80', '']:
            m = copy.deepcopy(self.m); m['images']['APP_IMAGE'] = value
            with self.assertRaises(ValueError): validate(m)
    def test_unsafe_tag(self):
        for value in ['main', 'prod-../../etc', 'prod-x\n']:
            m = copy.deepcopy(self.m); m['tag'] = value
            with self.assertRaises(ValueError): validate(m)
