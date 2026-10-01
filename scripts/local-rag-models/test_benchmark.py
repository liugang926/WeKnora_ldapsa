from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from benchmark import relevance_metrics, run


class RetrievalMetricTests(unittest.TestCase):
    def test_cross_document_evidence_scores_partial_top_one(self) -> None:
        metrics = relevance_metrics([101, 102, 103], {101, 102})
        self.assertEqual(metrics["recall_at_1"], 0.5)
        self.assertEqual(metrics["recall_at_3"], 1.0)
        self.assertEqual(metrics["ndcg_at_5"], 1.0)
        self.assertEqual(metrics["mrr_at_5"], 1.0)

    def test_no_answer_is_not_falsely_counted_as_retrieval_success(self) -> None:
        metrics = relevance_metrics([101, 102], set())
        self.assertIsNone(metrics["recall_at_5"])
        self.assertIsNone(metrics["ndcg_at_5"])

    def test_corpus_embeddings_are_batched_for_model_limit(self) -> None:
        documents = [{"id": index + 1, "scope": "all", "text": f"passage {index}"}
                     for index in range(42)]
        manifest = {
            "id": "batch-test", "classification": "synthetic", "documents": documents,
            "personas": {"employee": ["all"]},
            "cases": [{"id": 1, "category": "zh", "question": "passage 0?",
                       "answer": "yes", "evidence": [1]}],
        }
        embedding_sizes = []

        def fake_post(_base_url: str, path: str, _token: str, payload: dict) -> dict:
            if path == "/v1/embeddings":
                embedding_sizes.append(len(payload["input"]))
                return {"data": [{"index": index, "embedding": [1.0] + [0.0] * 511}
                                 for index in range(len(payload["input"]))]}
            return {"results": [{"index": index} for index in range(len(payload["documents"]))]}

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")
            with patch("benchmark.post", side_effect=fake_post):
                result = run(path, "http://example.invalid", "test-token")
        self.assertEqual(embedding_sizes, [16, 16, 10, 1])
        self.assertEqual(result["document_count"], 42)
        self.assertEqual(result["mean_recall_at_5"], 1.0)


if __name__ == "__main__":
    unittest.main()
