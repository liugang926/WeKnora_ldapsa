import io, json, pathlib, tempfile, unittest
from unittest.mock import patch
import update
class UpdateTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.base=pathlib.Path(self.tmp.name)
        self.patch=patch.object(update,'BASE',self.base); self.patch.start()
    def tearDown(self): self.patch.stop(); self.tmp.cleanup()
    def test_failure_marker_stops_before_network(self):
        (self.base/'UPDATE_FAILED').touch()
        with patch.object(update,'get') as get:
            with self.assertRaises(RuntimeError): update.main()
            get.assert_not_called()
    def test_drafts_and_prereleases_never_deploy(self):
        data=[{'draft':True,'prerelease':False,'tag_name':'prod-1'}, {'draft':False,'prerelease':True,'tag_name':'prod-2'}]
        with patch.object(update,'get',return_value=io.StringIO(json.dumps(data))),patch.object(update.subprocess,'run') as run:
            update.main();run.assert_not_called()
    def test_active_release_does_not_restart(self):
        (self.base/'active-release').write_text('prod-1\n')
        data=[{'draft':False,'prerelease':False,'tag_name':'prod-1','published_at':'2026-10-18T00:00:00Z'}]
        with patch.object(update,'get',return_value=io.StringIO(json.dumps(data))),patch.object(update.subprocess,'run') as run:
            update.main();run.assert_not_called()
    def test_cached_old_release_cannot_downgrade_database(self):
        target=self.base/'releases'/'prod-old';target.mkdir(parents=True)
        active=self.base/'current';active.mkdir()
        images={k:'sha256:'+'b'*64 for k in ['APP_IMAGE','UI_IMAGE','DOCREADER_IMAGE','POSTGRES_IMAGE','REDIS_IMAGE']}
        old={'schema':1,'tag':'prod-old','commit':'a'*40,'images':images}
        new=dict(old,tag='prod-current',commit='c'*40)
        (target/'manifest.json').write_text(json.dumps(old))
        (active/'manifest.json').write_text(json.dumps(new))
        releases=[{'draft':False,'prerelease':False,'tag_name':'prod-old','published_at':'2026-10-18T00:00:00Z'}]
        for status in ['behind','diverged']:
            responses=[io.StringIO(json.dumps(releases)),io.StringIO(json.dumps({'status':status}))]
            with patch.object(update,'get',side_effect=responses),patch.object(update.subprocess,'run') as run:
                with self.assertRaises(RuntimeError): update.main()
                run.assert_not_called()
    def test_interrupted_activation_preserves_pointer_and_stops_before_network(self):
        marker=self.base/'ACTIVATION_PENDING';marker.write_text('original backup pointer')
        with patch.object(update,'get') as get,patch.object(update.subprocess,'run') as run:
            with self.assertRaises(RuntimeError): update.main()
            get.assert_not_called();run.assert_not_called()
        self.assertEqual(marker.read_text(),'original backup pointer')
        self.assertTrue((self.base/'UPDATE_FAILED').exists())
    def test_journal_release_requires_reviewed_local_helper(self):
        target=self.base/'releases'/'prod-journal';target.mkdir(parents=True)
        manifest={'schema':1,'tag':'prod-journal','commit':'a'*40,
          'deployment_protocol':2,'required_features':['original_body_journal'],
          'images':{key:'sha256:'+'b'*64 for key in ['APP_IMAGE','UI_IMAGE','DOCREADER_IMAGE','POSTGRES_IMAGE','REDIS_IMAGE']}}
        (target/'manifest.json').write_text(json.dumps(manifest))
        releases=[{'draft':False,'prerelease':False,'tag_name':'prod-journal','published_at':'2026-10-19T00:00:00Z'}]
        with patch.object(update,'get',return_value=io.StringIO(json.dumps(releases))),patch.object(update.subprocess,'run') as run:
            with self.assertRaisesRegex(RuntimeError,'retention helper'):
                update.main()
            run.assert_not_called()
    def test_stage_only_verifies_images_without_activating_or_marking_active(self):
        target=self.base/'releases'/'prod-stage';target.mkdir(parents=True)
        images={key:'sha256:'+'b'*64 for key in ['APP_IMAGE','UI_IMAGE','DOCREADER_IMAGE','POSTGRES_IMAGE','REDIS_IMAGE']}
        manifest={'schema':1,'tag':'prod-stage','commit':'a'*40,'images':images}
        (target/'manifest.json').write_text(json.dumps(manifest))
        releases=[{'draft':False,'prerelease':False,'tag_name':'prod-stage','published_at':'2026-10-19T00:00:00Z'}]
        resolved={'local_images':images}
        with patch.object(update,'get',return_value=io.StringIO(json.dumps(releases))),patch.object(update,'resolve_images',return_value=resolved),patch.object(update.subprocess,'run') as run,patch('builtins.print'):
            update.main(stage_only=True)
            run.assert_not_called()
        self.assertEqual(json.loads((target/'resolved-images.json').read_text()),resolved)
        self.assertFalse((self.base/'current').exists())
        self.assertFalse((self.base/'active-release').exists())
    def test_asset_url_uses_the_strict_public_repository_api_endpoint(self):
        url = 'https://api.github.com/repos/liugang926/WeKnora_ldapsa/releases/assets/123456'
        self.assertEqual(update.asset_url({'url': url, 'browser_download_url': 'https://untrusted.invalid/ignored'}), url)
    def test_untrusted_or_browser_asset_urls_are_rejected(self):
        prefix = 'https://api.github.com/repos/liugang926/WeKnora_ldapsa/releases/assets/'
        urls = [None, 'https://github.com/liugang926/WeKnora_ldapsa/releases/download/prod-1/manifest.json',
                prefix + '123?token=private', prefix + '../123', prefix + 'abc',
                prefix + '123\n', prefix.replace('https:', 'http:') + '123',
                prefix.replace('api.github.com', 'api.github.com.evil.invalid') + '123',
                prefix.replace('liugang926', 'another-owner') + '123',
                prefix.replace('WeKnora_ldapsa', 'another-repository') + '123']
        for url in urls:
            with self.subTest(url=url):
                with self.assertRaisesRegex(RuntimeError, '^Untrusted asset URL$'):
                    update.asset_url({'url': url})
        with self.assertRaisesRegex(RuntimeError, '^Untrusted asset URL$'):
            update.asset_url({'browser_download_url': urls[1]})
