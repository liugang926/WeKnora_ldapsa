"""The pinned parser must receive both zip-9 security dependency updates."""
from pathlib import Path
import os
import shutil
import subprocess
import tempfile
import unittest


class PrepareAnyDocTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / 'scripts').mkdir()
        self.script = self.root / 'scripts/build-anydoc-lib.sh'
        shutil.copyfile(Path(__file__).with_name('build-anydoc-lib.sh'), self.script)
        self.crate = self.root / 'third_party/anydoc-go'
        self.crate.mkdir(parents=True)
        (self.crate / 'Cargo.toml').write_text('anydoc = "=0.1.9"\n')
        (self.crate / 'version.go').write_text('const Version = "0.1.9"\n')
        self.source = self.root / 'cargo/registry/src/local/anydoc-0.1.9'
        (self.source / 'src').mkdir(parents=True)
        (self.source / 'src/lib.rs').write_text('use render::markdown::document_to_markdown;\n')
        (self.source / 'Cargo.toml').write_text(
            '[dependencies.calamine]\nversion = "0.36.1"\nfeatures = ["dates"]\n'
            '[dependencies.zip]\nversion = "8.6.0"\nfeatures = ["deflate"]\n')

    def prepare(self):
        return subprocess.run(['bash', str(self.script), '--prepare-only'],
            env={**os.environ, 'CARGO_HOME': str(self.root / 'cargo'),
                 'TARGET': 'aarch64-unknown-linux-gnu'}, capture_output=True, text=True)

    def test_prepares_patched_parser_and_published_compatible_dependencies(self):
        result = self.prepare()
        self.assertEqual(result.returncode, 0, result.stderr)
        patched = self.crate / 'patched-anydoc'
        manifest = (patched / 'Cargo.toml').read_text()
        self.assertIn('version = "=9.0.5"', manifest)
        self.assertIn('version = "=0.37.0"', manifest)
        self.assertIn('features = ["dates"]', manifest)
        self.assertIn('features = ["deflate"]', manifest)
        self.assertTrue((patched / 'src/lib.rs').read_text().startswith('pub use '))
        self.assertEqual(self.prepare().returncode, 0)

    def test_refreshes_archive_prepared_before_security_update(self):
        patched = self.crate / 'patched-anydoc'
        shutil.copytree(self.source, patched)
        (patched / '.weknora-patched').write_text('0.1.9\n')
        self.assertEqual(self.prepare().returncode, 0)
        self.assertIn('version = "=9.0.5"', (patched / 'Cargo.toml').read_text())

    def test_dependency_drift_refuses_incomplete_patch(self):
        path = self.source / 'Cargo.toml'
        for content in ('[dependencies.zip]\nversion = "8.6.0"\n',
                        path.read_text().replace('0.36.1', '0.40.0')):
            with self.subTest(content=content):
                path.write_text(content)
                result = self.prepare()
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((self.crate / 'patched-anydoc/.weknora-patched').exists())

    def test_parser_identity_drift_refuses_mislabeled_documents(self):
        (self.crate / 'version.go').write_text('const Version = "0.1.8"\n')
        self.assertNotEqual(self.prepare().returncode, 0)
        self.assertFalse((self.crate / 'patched-anydoc').exists())


if __name__ == '__main__':
    unittest.main()
