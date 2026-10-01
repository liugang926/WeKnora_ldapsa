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

    def test_failed_task_reports_reason_without_tokens(self):
        detail = {"data": {"task": {"status": 3,
                                   "err_msg": "rerank failed with model-secret and user-secret"}}}
        with mock.patch.object(evaluation, "http_json", return_value=detail):
            with self.assertRaisesRegex(RuntimeError,
                                        "rerank failed with \\[redacted\\] and \\[redacted\\]") as failure:
                evaluation.wait_for_result("http://127.0.0.1:1234", "user-secret",
                                           "task-1", "model-secret")
        self.assertNotIn("user-secret", str(failure.exception))
        self.assertNotIn("model-secret", str(failure.exception))

    def test_synthetic_review_checks_answered_and_no_answer_cases(self):
        def save(_base, method, path, *, token, body):
            self.assertEqual(method, "PUT")
            self.assertEqual(token, "synthetic-jwt")
            question_id = int(path.split("/")[-2])
            return {"data": {"cases": [{"question_id": question_id, "review": body}]}}

        detail = {"cases": [{"question_id": 1, "reference_answer": "fictional"},
                            {"question_id": 2, "reference_answer": ""}]}
        with mock.patch.object(evaluation, "http_json", side_effect=save) as http_call:
            judgments = evaluation.review_synthetic_cases(
                "http://127.0.0.1:1234", "synthetic-jwt", "task-1", detail)
        self.assertEqual(len(judgments), 2)
        self.assertEqual(judgments[1]["citation_accuracy"], "fail")
        self.assertEqual(judgments[2]["abstention"], "fail")
        self.assertEqual(http_call.call_count, 2)

    def test_synthetic_chat_tariff_requires_complete_usage_and_exact_snapshot(self):
        detail = {"metric": {"execution_metrics": {
            "usage_accounting_version": 1, "chat_responses": 2,
            "usage_reported_responses": 2, "prompt_tokens": 1500,
            "completion_tokens": 200,
        }}}
        response = {"data": {"chat_cost": {
            "currency": "CNY", "tariff_version": "synthetic-v1",
            "chat_responses": 2, "usage_reported_responses": 2,
            "estimated_amount": 0.0019, "set_by": "admin",
        }}}
        with mock.patch.object(evaluation, "http_json", return_value=response) as call:
            estimate = evaluation.price_synthetic_chat(
                "http://127.0.0.1:1234", "synthetic-jwt", "task-1", detail)
        self.assertEqual(estimate, 0.0019)
        self.assertEqual(call.call_args.kwargs["body"]["tariff_version"], "synthetic-v1")
        detail["metric"]["execution_metrics"]["usage_reported_responses"] = 1
        with self.assertRaisesRegex(RuntimeError, "coverage is incomplete"):
            evaluation.price_synthetic_chat("http://127.0.0.1:1234", "synthetic-jwt", "task-1", detail)


if __name__ == "__main__":
    unittest.main()
