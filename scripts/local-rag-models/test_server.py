"""Offline model-loading boundary tests; never import real inference packages."""

from __future__ import annotations

import builtins
from contextlib import nullcontext
import importlib.util
import math
import os
from pathlib import Path
import sys
from types import ModuleType
import unittest
from unittest import mock


SCRIPT = Path(__file__).with_name("server.py")
EMBEDDING = ("BAAI/bge-small-zh-v1.5", "7999e1d3359715c523056ef9478215996d62a620")
RERANK = ("BAAI/bge-reranker-base", "2cfc18c9415c912f9d8155881c133215df768a70")


class ModelLoadingTests(unittest.TestCase):
    def setUp(self):
        self.embedding_loader = mock.Mock()
        self.rerank_loader = mock.Mock()
        self.mps_available = mock.Mock(return_value=True)
        torch = ModuleType("torch")
        torch.backends = mock.Mock()
        torch.backends.mps.is_available = self.mps_available
        torch.inference_mode = nullcontext
        transformers = ModuleType("sentence_transformers")
        transformers.SentenceTransformer = self.embedding_loader
        transformers.CrossEncoder = self.rerank_loader
        self.dependencies = {"torch": torch, "sentence_transformers": transformers}
        self.environment = {"RAG_MODEL_TOKEN": "synthetic-only-" + "x" * 32}

    def load(self, overrides=None):
        environment = {**self.environment, **(overrides or {})}
        spec = importlib.util.spec_from_file_location("local_rag_server_under_test", SCRIPT)
        module = importlib.util.module_from_spec(spec)
        with mock.patch.dict(os.environ, environment, clear=True), \
                mock.patch.dict(sys.modules, self.dependencies):
            spec.loader.exec_module(module)
        return module

    def assert_rejected_before_loading(self, overrides, message):
        original_import = builtins.__import__

        def reject_model_import(name, *args, **kwargs):
            if name in self.dependencies:
                raise AssertionError("Rejected configuration imported an inference dependency")
            return original_import(name, *args, **kwargs)

        with mock.patch.object(builtins, "__import__", side_effect=reject_model_import), \
                self.assertRaisesRegex(SystemExit, message):
            self.load(overrides)
        self.embedding_loader.assert_not_called()
        self.rerank_loader.assert_not_called()
        self.mps_available.assert_not_called()

    def test_approved_defaults_are_pinned_and_do_not_trust_custom_code(self):
        module = self.load()
        self.embedding_loader.assert_called_once_with(
            EMBEDDING[0], revision=EMBEDDING[1], device="mps", trust_remote_code=False)
        self.rerank_loader.assert_called_once_with(
            RERANK[0], revision=RERANK[1], device="mps", trust_remote_code=False)
        self.assertEqual(module.EMBEDDING_REVISION, EMBEDDING[1])
        self.assertEqual(module.RERANK_REVISION, RERANK[1])

    def test_explicit_approved_configuration_and_cpu_fallback(self):
        self.mps_available.return_value = False
        module = self.load({
            "RAG_EMBEDDING_MODEL": EMBEDDING[0], "RAG_EMBEDDING_REVISION": EMBEDDING[1],
            "RAG_RERANK_MODEL": RERANK[0], "RAG_RERANK_REVISION": RERANK[1],
            "RAG_TRUST_REMOTE_CODE": "false", "RAG_EMBEDDING_TRUST_REMOTE_CODE": "0",
            "RAG_RERANK_TRUST_REMOTE_CODE": "FALSE",
        })
        self.assertEqual(module.DEVICE, "cpu")
        self.assertIs(self.embedding_loader.call_args.kwargs["trust_remote_code"], False)
        self.assertIs(self.rerank_loader.call_args.kwargs["trust_remote_code"], False)

    def test_rejects_unapproved_repositories_and_local_paths_for_both_roles(self):
        for role in ("EMBEDDING", "RERANK"):
            for model in ("unapproved/custom-model", "/private/unapproved-model", "./local-model",
                          "BAAI/bge-small-zh-v1.5/", "", "https://example.invalid/model"):
                with self.subTest(role=role, model=model):
                    self.assert_rejected_before_loading(
                        {f"RAG_{role}_MODEL": model}, "approved BAAI snapshot")

    def test_rejects_unapproved_revisions_before_either_model_loads(self):
        for role in ("EMBEDDING", "RERANK"):
            for revision in ("main", "latest", "0" * 40, "", EMBEDDING[1] if role == "RERANK"
                             else RERANK[1]):
                with self.subTest(role=role, revision=revision):
                    self.assert_rejected_before_loading(
                        {f"RAG_{role}_REVISION": revision}, "approved BAAI snapshot")

    def test_rejects_remote_code_selection(self):
        for option in ("RAG_TRUST_REMOTE_CODE", "RAG_EMBEDDING_TRUST_REMOTE_CODE",
                       "RAG_RERANK_TRUST_REMOTE_CODE"):
            for value in ("true", "TRUE", "1", "yes", "on", "", "false "):
                with self.subTest(option=option, value=value):
                    self.assert_rejected_before_loading(
                        {option: value}, "Custom model code cannot be enabled")

    def test_invalid_configuration_does_not_echo_private_values(self):
        private_value = "/private/synthetic-path-do-not-log"
        with self.assertRaises(SystemExit) as failure:
            self.load({"RAG_RERANK_MODEL": private_value})
        self.assertNotIn(private_value, str(failure.exception))

    def test_short_token_is_rejected_before_loading(self):
        self.assert_rejected_before_loading(
            {"RAG_MODEL_TOKEN": ""}, "RAG_MODEL_TOKEN must be at least 32 characters")

    def test_embedding_protocol_preserves_normalized_batch(self):
        module = self.load()
        handler = object.__new__(module.Handler)
        handler._send = mock.Mock()
        self.embedding_loader.return_value.encode.return_value = [
            mock.Mock(**{"tolist.return_value": [1.0] + [0.0] * 511}),
        ]
        handler._embeddings({"model": EMBEDDING[0], "input": ["fictional input"]})
        self.embedding_loader.return_value.encode.assert_called_once_with(
            ["fictional input"], normalize_embeddings=True, batch_size=8)
        status, payload = handler._send.call_args.args
        self.assertEqual(status, 200)
        self.assertEqual(len(payload["data"][0]["embedding"]), 512)

    def test_rerank_protocol_preserves_finite_sorted_scores(self):
        module = self.load()
        handler = object.__new__(module.Handler)
        handler._send = mock.Mock()
        self.rerank_loader.return_value.predict.return_value = [0.2, 0.8]
        handler._rerank({"model": RERANK[0], "query": "fictional question",
                         "documents": ["first", "second"]})
        self.rerank_loader.return_value.predict.assert_called_once_with(
            [["fictional question", "first"], ["fictional question", "second"]], batch_size=8)
        status, payload = handler._send.call_args.args
        self.assertEqual(status, 200)
        self.assertEqual([row["index"] for row in payload["results"]], [1, 0])
        self.assertTrue(all(math.isfinite(row["relevance_score"]) for row in payload["results"]))

    def test_rerank_rejects_nonfinite_scores(self):
        module = self.load()
        handler = object.__new__(module.Handler)
        handler._send = mock.Mock()
        self.rerank_loader.return_value.predict.return_value = [float("nan")]
        with self.assertRaisesRegex(ValueError, "non-finite rerank score"):
            handler._rerank({"model": RERANK[0], "query": "fictional question",
                             "documents": ["first"]})
        handler._send.assert_not_called()


if __name__ == "__main__":
    unittest.main()
