import pathlib
import subprocess
import unittest

ROOT = pathlib.Path(__file__).resolve().parent


class BootStartupTests(unittest.TestCase):
    def test_start_script_has_valid_bash_syntax(self):
        subprocess.run(['bash', '-n', str(ROOT/'start.sh')], check=True)

    def test_docreader_directory_variable_is_defined_before_use(self):
        lines = (ROOT/'start.sh').read_text().splitlines()
        assignment = lines.index('data=/data/weknora-production')
        use = next(i for i, line in enumerate(lines) if 'runtime/docreader' in line)
        self.assertLess(assignment, use)

    def test_stack_and_timer_are_enabled_at_boot(self):
        service = (ROOT/'weknora-production.service').read_text()
        installer = (ROOT/'install.sh').read_text()
        self.assertIn('WantedBy=multi-user.target', service)
        self.assertIn("weknora-production-update.timer <<'UNIT'", installer)
        self.assertIn('WantedBy=timers.target', installer)
