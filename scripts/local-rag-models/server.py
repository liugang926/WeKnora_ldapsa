"""Local OpenAI-embedding/Cohere-rerank bridge for a single Mac test host.

Runs official BAAI model weights via SentenceTransformers/PyTorch. It is
deliberately not an internet-facing inference gateway: bind to a private
interface, require a random bearer token, and keep concurrency bounded.
"""

from __future__ import annotations

import hmac
import json
import math
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import torch
from sentence_transformers import CrossEncoder, SentenceTransformer


EMBEDDING_MODEL = os.environ.get("RAG_EMBEDDING_MODEL", "BAAI/bge-small-zh-v1.5")
RERANK_MODEL = os.environ.get("RAG_RERANK_MODEL", "BAAI/bge-reranker-base")
EMBEDDING_REVISION = os.environ.get("RAG_EMBEDDING_REVISION", "7999e1d3359715c523056ef9478215996d62a620")
RERANK_REVISION = os.environ.get("RAG_RERANK_REVISION", "2cfc18c9415c912f9d8155881c133215df768a70")
TOKEN = os.environ.get("RAG_MODEL_TOKEN", "")
HOST = os.environ.get("RAG_MODEL_HOST", "127.0.0.1")
PORT = int(os.environ.get("RAG_MODEL_PORT", "19090"))
MAX_INPUTS = 32
MAX_DOCUMENTS = 64
MAX_CHARS = 8192
MAX_BODY_BYTES = 2 * 1024 * 1024

if len(TOKEN) < 32:
    raise SystemExit("RAG_MODEL_TOKEN must be at least 32 characters")

DEVICE = "mps" if torch.backends.mps.is_available() else "cpu"
embedding = SentenceTransformer(EMBEDDING_MODEL, device=DEVICE, revision=EMBEDDING_REVISION)
reranker = CrossEncoder(RERANK_MODEL, device=DEVICE, revision=RERANK_REVISION)
inference_lock = threading.Lock()
request_slots = threading.BoundedSemaphore(8)


class Handler(BaseHTTPRequestHandler):
    server_version = "WeKnoraLocalRAG/1"

    def log_message(self, format_string: str, *args: object) -> None:
        # A path can contain query-string secrets, even though this API uses
        # JSON bodies. Log only the method and status, never request content.
        status = args[1] if len(args) > 1 else "-"
        print(f"{self.address_string()} {self.command} status={status}", flush=True)

    def _send(self, status: int, body: dict) -> None:
        encoded = json.dumps(body, ensure_ascii=False, allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(encoded)

    def _authorized(self) -> bool:
        supplied = self.headers.get("Authorization", "")
        return hmac.compare_digest(supplied, "Bearer " + TOKEN)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0 or length > MAX_BODY_BYTES:
            raise ValueError("invalid body size")
        data = json.loads(self.rfile.read(length))
        if not isinstance(data, dict):
            raise ValueError("body must be an object")
        return data

    def do_GET(self) -> None:
        if self.path == "/health":
            if not self._authorized():
                self._send(401, {"error": "unauthorized"})
                return
            self._send(200, {"status": "ok", "device": DEVICE,
                             "embedding_model": EMBEDDING_MODEL,
                             "embedding_revision": EMBEDDING_REVISION,
                             "rerank_model": RERANK_MODEL,
                             "rerank_revision": RERANK_REVISION})
            return
        self._send(404, {"error": "not found"})

    def do_POST(self) -> None:
        if not self._authorized():
            self._send(401, {"error": "unauthorized"})
            return
        if not request_slots.acquire(blocking=False):
            self._send(429, {"error": "model server busy"})
            return
        try:
            body = self._read_json()
            if self.path == "/v1/embeddings":
                self._embeddings(body)
            elif self.path == "/v1/rerank":
                self._rerank(body)
            else:
                self._send(404, {"error": "not found"})
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            self._send(400, {"error": str(exc)})
        except Exception:
            # Internal details may include text from the request or upstream.
            self._send(500, {"error": "model inference failed"})
        finally:
            request_slots.release()

    def _embeddings(self, body: dict) -> None:
        if body.get("model") != EMBEDDING_MODEL:
            raise ValueError("unknown embedding model")
        values = body.get("input")
        texts = [values] if isinstance(values, str) else values
        if not isinstance(texts, list) or not 1 <= len(texts) <= MAX_INPUTS:
            raise ValueError("input count out of range")
        if any(not isinstance(text, str) or not text or len(text) > MAX_CHARS for text in texts):
            raise ValueError("invalid embedding input")
        with inference_lock, torch.inference_mode():
            vectors = embedding.encode(texts, normalize_embeddings=True, batch_size=8)
        self._send(200, {"object": "list", "model": EMBEDDING_MODEL,
                         "data": [{"object": "embedding", "index": index,
                                   "embedding": vector.tolist()}
                                  for index, vector in enumerate(vectors)],
                         "usage": {"prompt_tokens": 0, "total_tokens": 0}})

    def _rerank(self, body: dict) -> None:
        if body.get("model") != RERANK_MODEL:
            raise ValueError("unknown rerank model")
        query = body.get("query")
        documents = body.get("documents")
        if not isinstance(query, str) or not query or len(query) > MAX_CHARS:
            raise ValueError("invalid rerank query")
        if not isinstance(documents, list) or not 1 <= len(documents) <= MAX_DOCUMENTS:
            raise ValueError("document count out of range")
        if any(not isinstance(doc, str) or not doc or len(doc) > MAX_CHARS for doc in documents):
            raise ValueError("invalid rerank document")
        with inference_lock, torch.inference_mode():
            raw = reranker.predict([[query, doc] for doc in documents], batch_size=8)
        results = []
        for index, value in enumerate(raw):
            score = float(value)
            if not math.isfinite(score):
                raise ValueError("non-finite rerank score")
            # CrossEncoder's one-label default applies sigmoid. Preserve
            # probabilities while defensively converting a raw logit.
            if score < 0 or score > 1:
                score = 1 / (1 + math.exp(-max(-50, min(50, score))))
            results.append({"index": index, "relevance_score": score})
        results.sort(key=lambda result: result["relevance_score"], reverse=True)
        self._send(200, {"results": results})


if __name__ == "__main__":
    print(f"Loading {EMBEDDING_MODEL} and {RERANK_MODEL} on {DEVICE}; serving {HOST}:{PORT}", flush=True)
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
