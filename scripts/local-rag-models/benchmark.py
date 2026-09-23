"""Run a deterministic retrieval-only smoke baseline against local real models.

This checks model quality and protocol shape, not WeKnora authorization or
answer faithfulness. Restricted synthetic passages are removed per persona
before retrieval; live AD/API permission tests remain separate.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import time
import urllib.request
from pathlib import Path


def post(base_url: str, path: str, token: str, payload: dict) -> dict:
    request = urllib.request.Request(
        base_url.rstrip("/") + path,
        json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        {"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=180) as response:
        return json.load(response)


def percentile(values: list[float], rank: float) -> float:
    ordered = sorted(values)
    index = math.ceil(len(ordered) * rank) - 1
    return ordered[max(0, index)]


def relevance_metrics(ordered_ids: list[int], evidence: set[int]) -> dict:
    if not evidence:
        return {"recall_at_1": None, "recall_at_3": None, "recall_at_5": None,
                "mrr_at_5": None, "ndcg_at_5": None}
    gains = [1 / math.log2(index + 2) for index, doc_id in enumerate(ordered_ids[:5])
             if doc_id in evidence]
    ideal = sum(1 / math.log2(index + 2) for index in range(min(len(evidence), 5)))
    first = next((index + 1 for index, doc_id in enumerate(ordered_ids[:5])
                  if doc_id in evidence), None)
    return {
        "recall_at_1": round(len(evidence & set(ordered_ids[:1])) / len(evidence), 3),
        "recall_at_3": round(len(evidence & set(ordered_ids[:3])) / len(evidence), 3),
        "recall_at_5": round(len(evidence & set(ordered_ids[:5])) / len(evidence), 3),
        "mrr_at_5": round(1 / first if first else 0, 3),
        "ndcg_at_5": round(sum(gains) / ideal, 3),
    }


def run(manifest_path: Path, base_url: str, token: str) -> dict:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    documents = manifest["documents"]
    cases = manifest["cases"]
    personas = manifest["personas"]
    embed_model = os.getenv("RAG_EMBEDDING_MODEL", "BAAI/bge-small-zh-v1.5")
    rank_model = os.getenv("RAG_RERANK_MODEL", "BAAI/bge-reranker-base")
    start = time.monotonic()
    corpus_response = post(base_url, "/v1/embeddings", token,
                           {"model": embed_model, "input": [doc["text"] for doc in documents]})
    corpus_vectors = {documents[i]["id"]: item["embedding"]
                      for i, item in enumerate(sorted(corpus_response["data"], key=lambda row: row["index"]))}
    dimensions = {len(vector) for vector in corpus_vectors.values()}
    if len(corpus_vectors) != len(documents) or dimensions != {512}:
        raise ValueError("embedding response has missing rows or unexpected dimensions")
    cases_out = []
    latencies = []
    for case in cases:
        case_start = time.monotonic()
        scopes = personas.get(case.get("persona"), personas["employee"])
        visible = [doc for doc in documents if doc["scope"] in scopes]
        query_vector = post(base_url, "/v1/embeddings", token,
                            {"model": embed_model, "input": [case["question"]]})["data"][0]["embedding"]
        # The server returns normalized embeddings, so dot product is cosine.
        ranked = sorted(visible, key=lambda doc: sum(a * b for a, b in zip(
            query_vector, corpus_vectors[doc["id"]])), reverse=True)
        candidates = ranked[:5]
        reranked = post(base_url, "/v1/rerank", token,
                        {"model": rank_model, "query": case["question"],
                         "documents": [doc["text"] for doc in candidates]})
        indices = [item["index"] for item in reranked["results"]]
        if sorted(indices) != list(range(len(candidates))):
            raise ValueError("rerank response has missing or repeated candidates")
        ordered = [candidates[item["index"]]["id"] for item in reranked["results"]]
        embedding_ordered = [doc["id"] for doc in candidates]
        evidence = set(case["evidence"])
        forbidden = set(case.get("must_not_access", []))
        latency_ms = round((time.monotonic() - case_start) * 1000, 1)
        latencies.append(latency_ms)
        cases_out.append({
            "id": case["id"], "category": case["category"], "persona": case.get("persona", "employee"),
            "retrieved_ids": ordered,
            "embedding_recall_at_5": relevance_metrics(embedding_ordered, evidence)["recall_at_5"],
            **relevance_metrics(ordered, evidence),
            "forbidden_visible": bool(forbidden & set(ordered)), "latency_ms": latency_ms,
        })
    scored = [item for item in cases_out if item["recall_at_5"] is not None]
    result = {
        "fixture_id": manifest["id"], "classification": manifest["classification"],
        "embedding_model": embed_model, "embedding_dimensions": dimensions.pop(),
        "rerank_model": rank_model, "case_count": len(cases), "document_count": len(documents),
        "mean_embedding_recall_at_5": round(statistics.mean(item["embedding_recall_at_5"] for item in scored), 3),
        "mean_recall_at_1": round(statistics.mean(item["recall_at_1"] for item in scored), 3),
        "mean_recall_at_3": round(statistics.mean(item["recall_at_3"] for item in scored), 3),
        "mean_recall_at_5": round(statistics.mean(item["recall_at_5"] for item in scored), 3),
        "mean_mrr_at_5": round(statistics.mean(item["mrr_at_5"] for item in scored), 3),
        "mean_ndcg_at_5": round(statistics.mean(item["ndcg_at_5"] for item in scored), 3),
        "forbidden_retrievals": sum(item["forbidden_visible"] for item in cases_out),
        "p50_latency_ms": percentile(latencies, .5), "p95_latency_ms": percentile(latencies, .95),
        "wall_time_s": round(time.monotonic() - start, 1), "cases": cases_out,
        "limits": "retrieval-only; persona filtering is harness-only; no generated-answer or live AD verdict",
    }
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--base-url", default="http://127.0.0.1:19090")
    args = parser.parse_args()
    token = os.getenv("RAG_MODEL_TOKEN", "")
    if not token:
        raise SystemExit("RAG_MODEL_TOKEN is required")
    print(json.dumps(run(args.manifest, args.base_url, token), ensure_ascii=False, indent=2))
