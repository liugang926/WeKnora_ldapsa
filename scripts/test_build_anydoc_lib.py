"""The pinned parser must receive zip-9 and calamine CFB security updates."""
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
        self.assertIn('version = "=0.38.0"', manifest)
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

    def test_refreshes_cache_prepared_with_vulnerable_calamine(self):
        patched = self.crate / 'patched-anydoc'
        shutil.copytree(self.source, patched)
        manifest = patched / 'Cargo.toml'
        manifest.write_text(manifest.read_text().replace('0.36.1', '=0.37.0'))
        marker = patched / '.weknora-patched'
        marker.write_text('0.1.9:zip9.0.5:calamine0.37.0\n')
        self.assertEqual(self.prepare().returncode, 0)
        self.assertIn('version = "=0.38.0"', manifest.read_text())
        self.assertNotIn('0.37.0', manifest.read_text())
        self.assertEqual(marker.read_text(), '0.1.9:zip9.0.5:calamine0.38.0\n')
        # The refreshed marker must also permit a subsequent cached build.
        (patched / 'cache-sentinel').write_text('retained')
        self.assertEqual(self.prepare().returncode, 0)
        self.assertTrue((patched / 'cache-sentinel').exists())

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

    def build_with_fake_toolchain(self, *, strip_exit=0,
                                  target='x86_64-unknown-linux-gnu', missing_objcopy=False):
        tools = self.root / 'bin'
        tools.mkdir(exist_ok=True)
        cargo = tools / 'cargo'
        cargo.write_text('#!/bin/sh\nmkdir -p "$(dirname "$STUB_ARCHIVE")"\n'
                         'printf native-with-bitcode > "$STUB_ARCHIVE"\n')
        cargo.chmod(0o755)
        objcopy = tools / 'objcopy'
        objcopy.write_text('#!/bin/sh\n'
                           'printf "%s\\n" "$@" >> "$STUB_STRIP_LOG"\n'
                           f'if [ {strip_exit} -ne 0 ]; then exit {strip_exit}; fi\n'
                           '[ "$1" = --remove-section=.llvmbc ] || exit 91\n'
                           '[ "$2" = --remove-section=.llvmcmd ] || exit 92\n'
                           'printf native-machine-code > "$3"\n')
        objcopy.chmod(0o755)
        return subprocess.run(['bash', str(self.script)], env={
            **os.environ, 'CARGO_HOME': str(self.root / 'cargo'),
            'PATH': str(tools) + os.pathsep + os.environ['PATH'],
            'TARGET': target, 'OBJCOPY': str(tools / 'missing') if missing_objcopy else str(objcopy),
            'STUB_ARCHIVE': str(self.crate / f'target/{target}/release/libanydoc_go.a'),
            'STUB_STRIP_LOG': str(self.root / 'strip.log'),
        }, capture_output=True, text=True)

    def test_linux_archive_is_sanitized_after_every_cached_build(self):
        for _ in range(2):
            result = self.build_with_fake_toolchain()
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual((self.crate / 'lib/linux_amd64_gnu/libanydoc_go.a').read_text(),
                             'native-machine-code')
        self.assertEqual((self.root / 'strip.log').read_text().count('--remove-section=.llvmbc'), 2)
        self.assertEqual((self.crate / 'target/x86_64-unknown-linux-gnu/release/libanydoc_go.a').read_text(),
                         'native-with-bitcode')

    def test_failed_linux_strip_preserves_previous_deliverable_and_cleans_staging(self):
        destination = self.crate / 'lib/linux_amd64_gnu'
        destination.mkdir(parents=True)
        (destination / 'libanydoc_go.a').write_text('previous-native-archive')
        result = self.build_with_fake_toolchain(strip_exit=7)
        self.assertEqual(result.returncode, 7, result.stderr)
        self.assertEqual((destination / 'libanydoc_go.a').read_text(), 'previous-native-archive')
        self.assertEqual(list(destination.iterdir()), [destination / 'libanydoc_go.a'])

    def test_missing_linux_objcopy_fails_before_cargo_build(self):
        result = self.build_with_fake_toolchain(missing_objcopy=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('objcopy not found', result.stderr)
        self.assertFalse((self.crate / 'target').exists())

    def test_non_linux_archive_does_not_use_gnu_objcopy(self):
        result = self.build_with_fake_toolchain(target='aarch64-apple-darwin', strip_exit=7)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.crate / 'lib/darwin_arm64/libanydoc_go.a').read_text(),
                         'native-with-bitcode')
        self.assertFalse((self.root / 'strip.log').exists())


if __name__ == '__main__':
    unittest.main()
