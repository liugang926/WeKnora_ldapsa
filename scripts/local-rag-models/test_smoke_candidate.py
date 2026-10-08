"""Offline safety checks for the optional real-model candidate smoke."""

import importlib.util
from pathlib import Path
import stat
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).with_name("smoke_candidate.py")
SPEC = importlib.util.spec_from_file_location("smoke_candidate", SCRIPT)
smoke = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(smoke)


class CandidateSmokeSafetyTests(unittest.TestCase):
    def test_private_file_is_atomic_and_not_overwritten(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "model.env"
            smoke.write_private(target, "RAG_MODEL_TOKEN=synthetic-only\n")
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)
            with self.assertRaises(FileExistsError):
                smoke.write_private(target, "replacement")
            self.assertEqual(target.read_text(), "RAG_MODEL_TOKEN=synthetic-only\n")

    def test_generated_model_config_does_not_contain_token(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            token = "synthetic-token-" + "x" * 40
            compose_file = smoke.make_compose(root, "candidate:test", token)
            self.assertEqual(stat.S_IMODE(compose_file.stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE((root / "builtin-models.yaml").stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE((root / "model.env").stat().st_mode), 0o600)
            self.assertNotIn(token, compose_file.read_text())
            self.assertNotIn(token, (root / "builtin-models.yaml").read_text())
            self.assertIn(token, (root / "model.env").read_text())

    def test_existing_project_volume_is_rejected(self):
        def fake_docker(*args, **_kwargs):
            return "owned-volume" if args[:2] == ("volume", "ls") else ""

        with mock.patch.object(smoke, "docker", side_effect=fake_docker):
            with self.assertRaisesRegex(RuntimeError, "already has volume resources"):
                smoke.ensure_unused_project("rag-bge-test")


if __name__ == "__main__":
    unittest.main()
