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
