#!/usr/bin/env python3
"""Exercise a local app image with real BGE models in a disposable stack.

Only fictional text is sent to the models. The model service must already be
running on the Docker host; this script never downloads weights or calls a chat
model. It creates a fresh Compose project, then removes only that project.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import tempfile
import time
from urllib import error, request


EMBED_NAME = "BAAI/bge-small-zh-v1.5"
RERANK_NAME = "BAAI/bge-reranker-base"
QUESTION = "采购金额超过五百元由谁审批"
EVIDENCE = "采购金额超过五百元由部门主管审批。"
DISTRACTOR = "食堂晚餐供应时间为十八点。"


def docker(*args: str, timeout: int = 120) -> str:
    result = subprocess.run(
        ["docker", *args], text=True, capture_output=True, check=False,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Docker command failed ({args[0]}, exit {result.returncode})")
    return result.stdout.strip()


def compose(project: str, file: Path, *args: str, timeout: int = 180) -> str:
    return docker("compose", "-p", project, "-f", str(file), *args, timeout=timeout)


def ensure_unused_project(project: str) -> None:
    filters = ("container", "network", "volume")
    for kind in filters:
        if kind == "container":
            existing = docker("ps", "-a", "--filter",
                              f"label=com.docker.compose.project={project}",
                              "--format", "{{.ID}}")
        else:
            existing = docker(kind, "ls", "--filter",
                              f"label=com.docker.compose.project={project}",
                              "--format", "{{.Name}}" if kind == "volume" else "{{.ID}}")
        if existing:
            raise RuntimeError(f"generated Compose project already has {kind} resources")


def image_metadata(tag: str, expected_revision: str | None,
                   expected_patch: str | None) -> dict[str, str]:
    inspected = json.loads(docker("image", "inspect", tag))
    if len(inspected) != 1:
        raise RuntimeError("candidate image did not resolve uniquely")
    item = inspected[0]
    labels = item.get("Config", {}).get("Labels") or {}
    revision = labels.get("org.opencontainers.image.revision", "")
    patch = labels.get("io.github.liugang926.weknora.nextcloud-patch-sha256", "")
    if expected_revision and revision != expected_revision:
        raise RuntimeError("candidate source revision label mismatch")
    if expected_patch and patch != expected_patch:
        raise RuntimeError("candidate Nextcloud patch label mismatch")
    return {"image_id": item["Id"], "revision": revision, "patch_sha256": patch}


def write_private(path: Path, contents: str) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as private_file:
        private_file.write(contents)


def make_compose(scratch: Path, image: str, token: str) -> Path:
    db_password = secrets.token_hex(16)
    model_config = scratch / "builtin-models.yaml"
    write_private(model_config, """builtin_models:
  - id: builtin-local-bge-small-zh-v15
    type: Embedding
    source: remote
    is_default: true
    name: BAAI/bge-small-zh-v1.5
    parameters:
      base_url: http://host.docker.internal:19090/v1
      api_key: ${RAG_MODEL_TOKEN}
      provider: generic
      embedding_parameters:
        dimension: 512
        truncate_prompt_tokens: 0
  - id: builtin-local-bge-reranker-base
    type: Rerank
    source: remote
    is_default: true
    name: BAAI/bge-reranker-base
    parameters:
      base_url: http://host.docker.internal:19090/v1
      api_key: ${RAG_MODEL_TOKEN}
      provider: generic
""")
    model_env = scratch / "model.env"
    write_private(model_env, f"RAG_MODEL_TOKEN={token}\n")
    spec = {
        "services": {
            "db": {
                "image": "paradedb/paradedb:v0.22.6-pg17",
                "environment": {
                    "POSTGRES_USER": "weknora",
                    "POSTGRES_PASSWORD": db_password,
                    "POSTGRES_DB": "weknora",
                },
                "healthcheck": {
                    "test": ["CMD-SHELL", "pg_isready -U weknora -d weknora"],
                    "interval": "3s", "timeout": "5s", "retries": 30,
                },
            },
            "redis": {"image": "redis:7.0-alpine"},
            "app": {
                "image": image,
                "pull_policy": "never",
                "env_file": [str(model_env)],
                "ports": ["127.0.0.1::8080"],
                "extra_hosts": ["host.docker.internal:host-gateway"],
                "environment": {
                    "GIN_MODE": "release", "LOG_LEVEL": "warn",
                    "DB_DRIVER": "postgres", "DB_HOST": "db", "DB_PORT": "5432",
                    "DB_USER": "weknora", "DB_PASSWORD": db_password,
                    "DB_NAME": "weknora", "REDIS_ADDR": "redis:6379",
                    "RETRIEVE_DRIVER": "postgres", "STORAGE_TYPE": "local",
                    "LOCAL_STORAGE_BASE_DIR": "/tmp/weknora-files",
                    "AUTO_MIGRATE": "true", "JWT_SECRET": secrets.token_hex(32),
                    "SYSTEM_AES_KEY": secrets.token_hex(16),
                    "SSRF_WHITELIST_EXTRA": "host.docker.internal",
                    "BUILTIN_MODELS_CONFIG": "/run/config/builtin-models.yaml",
                    "LDAP_ENABLED": "false",
                    "WEKNORA_SANDBOX_DOCKER_ENABLED": "false",
                },
                "volumes": [f"{model_config}:/run/config/builtin-models.yaml:ro"],
                "depends_on": {"db": {"condition": "service_healthy"},
                               "redis": {"condition": "service_started"}},
                "healthcheck": {
                    "test": ["CMD", "curl", "-fsS", "http://localhost:8080/health"],
                    "interval": "5s", "timeout": "5s", "retries": 30,
                    "start_period": "30s",
                },
            },
        },
    }
    path = scratch / "compose.json"
    write_private(path, json.dumps(spec, ensure_ascii=False))
    return path


def http_json(base: str, method: str, path: str, *, body: dict | None = None,
              token: str | None = None, fields: dict[str, str] | None = None,
              timeout: int = 45) -> dict:
    headers: dict[str, str] = {}
    data: bytes | None = None
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    if fields is not None:
        boundary = "rag-bge-" + secrets.token_hex(12)
        parts = []
        for key, value in fields.items():
            parts.append((f"--{boundary}\r\nContent-Disposition: form-data; "
                          f'name="{key}"\r\n\r\n{value}\r\n').encode("utf-8"))
        parts.append(f"--{boundary}--\r\n".encode("ascii"))
        data = b"".join(parts)
        headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
    req = request.Request(base + path, data=data, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=timeout) as response:
            return json.load(response)
    except error.HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code} at {path}") from None


def run_checks(base: str, project: str, file: Path) -> dict:
    user_suffix = secrets.token_hex(5)
    password = "A1" + secrets.token_hex(14)
    email = f"bge-{user_suffix}@example.invalid"
    http_json(base, "POST", "/api/v1/auth/register", body={
        "username": f"bge-{user_suffix}", "email": email, "password": password,
    })
    login = http_json(base, "POST", "/api/v1/auth/login", body={
        "email": email, "password": password,
    })
    jwt = login.get("token")
    if not isinstance(jwt, str) or not jwt:
        raise RuntimeError("synthetic login did not return a token")
    models = http_json(base, "GET", "/api/v1/models", token=jwt).get("data")
    if not isinstance(models, list):
        raise RuntimeError("model listing is malformed")
    by_name = {model.get("name"): model for model in models}
    if EMBED_NAME not in by_name or RERANK_NAME not in by_name:
        raise RuntimeError("built-in BGE models are missing")
    embed_id = by_name[EMBED_NAME]["id"]
    rerank_id = by_name[RERANK_NAME]["id"]

    embedding = http_json(base, "POST", f"/api/v1/models/{embed_id}/debug",
                          token=jwt, fields={"input": QUESTION})["data"]
    if not embedding.get("ok") or embedding.get("observations", {}).get("dimension") != 512 \
            or len(embedding.get("raw_response") or []) != 512:
        raise RuntimeError("BGE Embedding did not return 512 dimensions")
    rerank = http_json(base, "POST", f"/api/v1/models/{rerank_id}/debug",
                       token=jwt, fields={"input": QUESTION,
                                          "documents": json.dumps([EVIDENCE, DISTRACTOR],
                                                                  ensure_ascii=False)})["data"]
    ranked = rerank.get("raw_response") or []
    if not rerank.get("ok") or len(ranked) != 2 or ranked[0].get("index") != 0 \
            or ranked[0].get("relevance_score", 0) <= ranked[1].get("relevance_score", 1):
        raise RuntimeError("BGE ReRank did not order the synthetic evidence first")

    kb = http_json(base, "POST", "/api/v1/knowledge-bases", token=jwt,
                   body={"name": "Synthetic BGE candidate", "type": "document",
                         "embedding_model_id": embed_id})["data"]
    kb_id = kb["id"]
    knowledge = http_json(base, "POST", f"/api/v1/knowledge-bases/{kb_id}/knowledge/manual",
                          token=jwt, body={"title": "虚构采购条款",
                                           "content": "# 虚构采购条款\n\n" + EVIDENCE + DISTRACTOR,
                                           "status": "publish"})["data"]
    knowledge_id = knowledge["id"]
    deadline = time.monotonic() + 75
    while time.monotonic() < deadline:
        current = http_json(base, "GET", f"/api/v1/knowledge/{knowledge_id}", token=jwt)["data"]
        if current.get("parse_status") == "completed" and current.get("enable_status") == "enabled":
            break
        if current.get("parse_status") == "failed":
            raise RuntimeError("synthetic knowledge parse failed")
        time.sleep(2)
    else:
        raise RuntimeError("synthetic knowledge did not finish parsing")

    search = http_json(base, "POST", f"/api/v1/knowledge-bases/{kb_id}/hybrid-search",
                       token=jwt, body={"query_text": QUESTION,
                                        "disable_keywords_match": True,
                                        "vector_threshold": 0, "match_count": 5})
    hits = search.get("data")
    if not search.get("success") or not isinstance(hits, list) or not hits \
            or not any("部门主管审批" in json.dumps(hit, ensure_ascii=False) for hit in hits):
        raise RuntimeError("vector-only search missed the synthetic evidence")

    rows = compose(project, file, "exec", "-T", "db", "psql", "-U", "weknora", "-d", "weknora",
                   "-Atc", "select dimension,count(*),count(*) filter (where is_enabled) "
                           "from embeddings group by dimension order by dimension")
    dimensions = [row.split("|") for row in rows.splitlines() if row]
    if len(dimensions) != 1 or dimensions[0][0] != "512" \
            or int(dimensions[0][2]) < 1:
        raise RuntimeError("database lacks enabled 512-dimensional embeddings")
    return {"embedding_dimension": 512,
            "rerank_positive_first": True,
            "rerank_scores": [ranked[0]["relevance_score"], ranked[1]["relevance_score"]],
            "knowledge_parse": "completed", "vector_only_hits": len(hits),
            "enabled_512d_vectors": int(dimensions[0][2])}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True, help="local app image tag; never pulled")
    parser.add_argument("--token-file", required=True,
                        help="0600 file holding the local BGE service bearer token")
    parser.add_argument("--expected-revision", help="optional exact image source label")
    parser.add_argument("--expected-patch-sha", help="optional exact Nextcloud patch label")
    args = parser.parse_args()
    token_file = Path(args.token_file).resolve(strict=True)
    if token_file.stat().st_mode & 0o077:
        raise SystemExit("token file must not be accessible by group or others")
    token = token_file.read_text(encoding="utf-8").strip()
    if not re.fullmatch(r"[A-Za-z0-9._-]{16,256}", token):
        raise SystemExit("token file has an unsupported format")
    metadata = image_metadata(args.image, args.expected_revision, args.expected_patch_sha)
    project = "rag-bge-" + secrets.token_hex(5)
    ensure_unused_project(project)
    with tempfile.TemporaryDirectory(prefix="weknora-bge-candidate-") as folder:
        scratch = Path(folder)
        spec = make_compose(scratch, args.image, token)
        started = False
        result: dict | None = None
        try:
            started = True
            compose(project, spec, "up", "-d", "--wait", "db", "redis", "app", timeout=240)
            app_container = compose(project, spec, "ps", "-q", "app")
            if not app_container or docker("inspect", "--format", "{{.Image}}",
                                           app_container) != metadata["image_id"]:
                raise RuntimeError("running app image differs from the inspected candidate")
            published = compose(project, spec, "port", "app", "8080")
            if not re.fullmatch(r"127\.0\.0\.1:\d+", published):
                raise RuntimeError("candidate app is not bound to loopback")
            result = run_checks("http://" + published, project, spec)
        finally:
            if started:
                compose(project, spec, "down", "--volumes", timeout=120)
                ensure_unused_project(project)
        if result is None:
            raise RuntimeError("candidate smoke did not produce a result")
        print(json.dumps({"ok": True, "image": metadata, "checks": result,
                          "owned_project_removed": True}, ensure_ascii=False))


if __name__ == "__main__":
    main()
