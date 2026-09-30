#!/usr/bin/env python3
"""Check durable synthetic evaluation on an isolated real-BGE app image.

The corpus is entirely fictional. A loopback-only deterministic chat stub is
used so this smoke cannot be mistaken for answer-quality acceptance or incur
external LLM charges. No shared Compose project or database is touched.
"""

from __future__ import annotations

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import re
import secrets
import tempfile
import threading
import time
from urllib import error, request

from smoke_candidate import (compose, docker, ensure_unused_project, http_json,
                             image_metadata, make_compose)


DATASET_TOTALS = {"synthetic-zh-v1": 16, "synthetic-enterprise-zh-v2": 70}
DATASET_DIR = Path(__file__).resolve().parents[2] / "dataset" / "benchmarks"
CHAT_MODEL_ID = "builtin-local-synthetic-evaluation-chat"
ANSWER = "这是完全虚构的评估流程测试回答。"


class ChatStub(BaseHTTPRequestHandler):
    requests_seen = 0
    request_lock = threading.Lock()

    def log_message(self, _format: str, *_args: object) -> None:
        pass  # Never log prompts or credentials.

    def do_POST(self) -> None:
        if self.path != "/v1/chat/completions":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        if length < 1 or length > 1024 * 1024:
            self.send_error(413)
            return
        try:
            payload = json.loads(self.rfile.read(length))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self.send_error(400)
            return
        if not isinstance(payload, dict):
            self.send_error(400)
            return
        with self.request_lock:
            type(self).requests_seen += 1
        usage = {"prompt_tokens": 10, "completion_tokens": 8, "total_tokens": 18}
        if payload.get("stream"):
            packets = [
                {"choices": [{"index": 0, "delta": {"role": "assistant", "content": ANSWER}}]},
                {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                 "usage": usage},
            ]
            body = b"".join(("data: " + json.dumps(packet, ensure_ascii=False) + "\n\n")
                            .encode("utf-8") for packet in packets) + b"data: [DONE]\n\n"
            content_type = "text/event-stream"
        else:
            body = json.dumps({
                "id": "synthetic-evaluation-stub", "object": "chat.completion",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": ANSWER},
                             "finish_reason": "stop"}], "usage": usage,
            }, ensure_ascii=False).encode("utf-8")
            content_type = "application/json"
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def configure_evaluation(spec_path: Path, stub_port: int) -> None:
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    app = spec["services"]["app"]
    app["environment"]["EVALUATION_DATASET_DIR"] = "/app/dataset/benchmarks"
    app["environment"]["EVALUATION_MAX_CONCURRENCY"] = "2"
    app["volumes"].append(f"{DATASET_DIR}:/app/dataset/benchmarks:ro")
    spec_path.write_text(json.dumps(spec, ensure_ascii=False), encoding="utf-8")
    config_path = spec_path.parent / "builtin-models.yaml"
    with config_path.open("a", encoding="utf-8") as config:
        config.write(f"""  - id: {CHAT_MODEL_ID}
    type: KnowledgeQA
    source: remote
    is_default: true
    name: synthetic-evaluation-chat-stub
    parameters:
      base_url: http://host.docker.internal:{stub_port}/v1
      api_key: synthetic-only
      provider: generic
""")


def wait_for_result(base: str, token: str, task_id: str) -> dict:
    deadline = time.monotonic() + 360
    while time.monotonic() < deadline:
        detail = http_json(base, "GET", f"/api/v1/evaluation?task_id={task_id}",
                           token=token, timeout=30)["data"]
        task = detail.get("task") or {}
        if task.get("status") == 2:
            return detail
        if task.get("status") == 3:
            raise RuntimeError("synthetic evaluation task failed")
        time.sleep(3)
    raise RuntimeError("synthetic evaluation task did not finish within 360 seconds")


def wait_for_app_restart(project: str, spec: Path) -> str:
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        container = compose(project, spec, "ps", "-q", "app")
        if not container or docker("inspect", "--format", "{{.State.Health.Status}}",
                                   container) != "healthy":
            time.sleep(2)
            continue
        published = compose(project, spec, "port", "app", "8080")
        if not re.fullmatch(r"127\.0\.0\.1:\d+", published):
            raise RuntimeError("restarted app is not bound to loopback")
        base = "http://" + published
        try:
            with request.urlopen(base + "/health", timeout=3) as response:
                if response.status == 200:
                    return base
        except (error.URLError, TimeoutError):
            pass
        time.sleep(2)
    raise RuntimeError("candidate app did not become healthy after restart")


def run_checks(base: str, project: str, spec: Path, dataset_id: str) -> dict:
    expected_total = DATASET_TOTALS[dataset_id]
    suffix = secrets.token_hex(5)
    email = f"rag-eval-{suffix}@example.invalid"
    password = "A1" + secrets.token_hex(14)
    http_json(base, "POST", "/api/v1/auth/register", body={
        "username": f"rag-eval-{suffix}", "email": email, "password": password,
    })
    token = http_json(base, "POST", "/api/v1/auth/login", body={
        "email": email, "password": password,
    }).get("token")
    if not isinstance(token, str) or not token:
        raise RuntimeError("synthetic login did not return a token")
    models = http_json(base, "GET", "/api/v1/models", token=token)["data"]
    ids = {model.get("name"): model.get("id") for model in models}
    if not all(ids.get(name) for name in (
        "BAAI/bge-small-zh-v1.5", "BAAI/bge-reranker-base",
        "synthetic-evaluation-chat-stub",
    )):
        raise RuntimeError("synthetic evaluation models are missing")
    created = http_json(base, "POST", "/api/v1/evaluation", token=token, body={
        "dataset_id": dataset_id,
        "embedding_id": ids["BAAI/bge-small-zh-v1.5"],
        "rerank_id": ids["BAAI/bge-reranker-base"],
        "chat_id": ids["synthetic-evaluation-chat-stub"],
    }, timeout=60)["data"]
    task_id = (created.get("task") or {}).get("id")
    if not isinstance(task_id, str) or not task_id:
        raise RuntimeError("evaluation did not return a task ID")
    detail = wait_for_result(base, token, task_id)
    task = detail["task"]
    metric = detail.get("metric") or {}
    if task.get("total") != expected_total or task.get("finished") != expected_total \
            or not re.fullmatch(r"[0-9a-f]{64}", task.get("dataset_sha256") or "") \
            or task.get("embedding_model_id") != ids["BAAI/bge-small-zh-v1.5"] \
            or task.get("rerank_model_id") != ids["BAAI/bge-reranker-base"] \
            or metric.get("metric_version") != 2 \
            or len(detail.get("cases") or []) != expected_total:
        raise RuntimeError("durable evaluation has missing cases, model IDs, or metric version")
    if ChatStub.requests_seen < 1:
        raise RuntimeError("synthetic chat stub was not called")
    before = {"task_id": task_id, "dataset_id": dataset_id,
              "dataset_sha256": task["dataset_sha256"],
              "total": task["total"], "finished": task["finished"],
              "metric_version": metric["metric_version"]}
    compose(project, spec, "restart", "app", timeout=90)
    base = wait_for_app_restart(project, spec)
    restored = http_json(base, "GET", f"/api/v1/evaluation?task_id={task_id}",
                         token=token, timeout=30)["data"]
    if (restored.get("task") or {}).get("dataset_sha256") != before["dataset_sha256"] \
            or (restored.get("task") or {}).get("status") != 2 \
            or len(restored.get("cases") or []) != expected_total:
        raise RuntimeError("evaluation result was not preserved across app restart")
    history = http_json(base, "GET", "/api/v1/evaluation", token=token)["data"]
    if not isinstance(history, list) or not any((item.get("task") or {}).get("id") == task_id
                                                 for item in history):
        raise RuntimeError("evaluation result is absent from durable history")
    return {**before, "chat_stub_requests": ChatStub.requests_seen,
            "restored_after_restart": True}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True, help="local app image tag; never pulled")
    parser.add_argument("--token-file", required=True, help="0600 local BGE bearer token file")
    parser.add_argument("--expected-revision", help="exact image source label")
    parser.add_argument("--expected-patch-sha", help="exact Nextcloud patch label")
    parser.add_argument("--dataset-id", choices=tuple(DATASET_TOTALS),
                        default="synthetic-zh-v1", help="committed fictional fixture only")
    args = parser.parse_args()
    token_file = Path(args.token_file).resolve(strict=True)
    if token_file.stat().st_mode & 0o077:
        raise SystemExit("token file must not be accessible by group or others")
    model_token = token_file.read_text(encoding="utf-8").strip()
    if not re.fullmatch(r"[A-Za-z0-9._-]{16,256}", model_token):
        raise SystemExit("token file has an unsupported format")
    metadata = image_metadata(args.image, args.expected_revision, args.expected_patch_sha)
    project = "rag-eval-" + secrets.token_hex(5)
    ensure_unused_project(project)
    ChatStub.requests_seen = 0
    server = ThreadingHTTPServer(("127.0.0.1", 0), ChatStub)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with tempfile.TemporaryDirectory(prefix="weknora-eval-candidate-") as folder:
            scratch = Path(folder)
            spec = make_compose(scratch, args.image, model_token)
            configure_evaluation(spec, server.server_port)
            started = False
            result: dict | None = None
            try:
                started = True
                compose(project, spec, "up", "-d", "--wait", "db", "redis", "app", timeout=240)
                app_container = compose(project, spec, "ps", "-q", "app")
                if not app_container or docker("inspect", "--format", "{{.Image}}",
                                               app_container) != metadata["image_id"]:
                    raise RuntimeError("running app image differs from inspected candidate")
                published = compose(project, spec, "port", "app", "8080")
                if not re.fullmatch(r"127\.0\.0\.1:\d+", published):
                    raise RuntimeError("candidate app is not bound to loopback")
                result = run_checks("http://" + published, project, spec, args.dataset_id)
            finally:
                if started:
                    compose(project, spec, "down", "--volumes", timeout=120)
                    ensure_unused_project(project)
            if result is None:
                raise RuntimeError("candidate evaluation did not produce a result")
            print(json.dumps({"ok": True, "image": metadata, "checks": result,
                              "owned_project_removed": True}, ensure_ascii=False))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


if __name__ == "__main__":
    main()
