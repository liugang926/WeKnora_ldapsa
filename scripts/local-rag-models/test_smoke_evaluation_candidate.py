"""Offline tests for the synthetic durable-evaluation candidate smoke."""

from http.server import ThreadingHTTPServer
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock
from urllib import request

import smoke_evaluation_candidate as evaluation
from smoke_candidate import make_compose


class EvaluationCandidateTest(unittest.TestCase):
    def test_compose_uses_private_fixture_and_loopback_chat(self):
        with tempfile.TemporaryDirectory() as folder:
            scratch = Path(folder)
            spec_path = make_compose(scratch, "synthetic-app:test", "a" * 32)
            evaluation.configure_evaluation(spec_path, 43210)
            spec = json.loads(spec_path.read_text(encoding="utf-8"))
            app = spec["services"]["app"]
            self.assertEqual(app["environment"]["EVALUATION_MAX_CONCURRENCY"], "2")
            self.assertIn(f"{evaluation.DATASET_DIR}:/app/dataset/benchmarks:ro",
                          app["volumes"])
            self.assertEqual(spec_path.stat().st_mode & 0o777, 0o600)
            config = (scratch / "builtin-models.yaml").read_text(encoding="utf-8")
            self.assertIn("http://host.docker.internal:43210/v1", config)
            self.assertNotIn("a" * 32, config)

    def test_chat_stub_supports_stream_and_nonstream(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), evaluation.ChatStub)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            for streamed in (False, True):
                with self.subTest(streamed=streamed):
                    payload = json.dumps({"model": "synthetic", "messages": [],
                                          "stream": streamed}).encode("utf-8")
                    endpoint = f"http://127.0.0.1:{server.server_port}/v1/chat/completions"
                    with request.urlopen(request.Request(endpoint, data=payload,
                                                         headers={"Content-Type": "application/json"}),
                                         timeout=3) as response:
                        body = response.read().decode("utf-8")
                    self.assertIn(evaluation.ANSWER, body)
                    if streamed:
                        self.assertIn("data: [DONE]", body)
                    else:
                        self.assertEqual(json.loads(body)["choices"][0]["finish_reason"], "stop")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)

    def test_restart_rechecks_current_port(self):
        class Healthy:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        with mock.patch.object(evaluation, "compose", side_effect=[
            "container-id", "127.0.0.1:47321",
        ]), mock.patch.object(evaluation, "docker", return_value="healthy"), \
                mock.patch.object(evaluation.request, "urlopen", return_value=Healthy()) as opened:
            base = evaluation.wait_for_app_restart("project", Path("compose.json"))
        self.assertEqual(base, "http://127.0.0.1:47321")
        opened.assert_called_once_with(base + "/health", timeout=3)


if __name__ == "__main__":
    unittest.main()
