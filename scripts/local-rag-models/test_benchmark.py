from __future__ import annotations

import unittest

from benchmark import relevance_metrics


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


if __name__ == "__main__":
    unittest.main()
