import copy, unittest
from validate_release import validate
class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.m = {'schema': 1, 'commit': 'a'*40, 'tag': 'prod-20261018.1',
          'images': {k: 'sha256:'+'b'*64 for k in ['APP_IMAGE','UI_IMAGE','POSTGRES_IMAGE','REDIS_IMAGE']}}
    def test_valid(self):
        self.assertEqual(validate(self.m), self.m)
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
