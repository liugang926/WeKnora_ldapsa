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
        images={k:'sha256:'+'b'*64 for k in ['APP_IMAGE','UI_IMAGE','POSTGRES_IMAGE','REDIS_IMAGE']}
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
