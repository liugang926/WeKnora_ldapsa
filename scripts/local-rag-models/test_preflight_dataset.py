from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from preflight_dataset import preflight


BENCHMARKS = Path(__file__).resolve().parents[2] / "dataset" / "benchmarks"


class DatasetPreflightTests(unittest.TestCase):
    def test_synthetic_fixtures_report_only_counts_and_hashes(self) -> None:
        for dataset_id, questions in (("synthetic-zh-v1", 16),
                                      ("synthetic-enterprise-zh-v2", 70)):
            report = preflight(BENCHMARKS / dataset_id)
            self.assertEqual(report["dataset_id"], dataset_id)
            self.assertEqual(set(report), {"dataset_id", "counts", "file_sha256", "note"})
            self.assertEqual(report["counts"]["queries"], questions)
            self.assertEqual(len(report["file_sha256"]), 5)
            self.assertTrue(all(len(value) == 64 for value in report["file_sha256"].values()))

    def test_rejects_duplicate_and_dangling_relations(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            dataset = Path(temporary) / "fixture"
            dataset.mkdir()
            rows = {
                "corpus": {"id": [1], "text": ["fictional passage"]},
                "queries": {"id": [2], "text": ["fictional question"]},
                "answers": {"id": [3], "text": ["fictional answer"]},
                "qrels": {"qid": [2, 2], "pid": [1, 1]},
                "qas": {"qid": [2], "aid": [3]},
            }
            for name, columns in rows.items():
                pq.write_table(pa.table(columns), dataset / f"{name}.parquet")
            with self.assertRaisesRegex(ValueError, "duplicate question/evidence"):
                preflight(dataset)
            pq.write_table(pa.table({"qid": [2], "pid": [99]}), dataset / "qrels.parquet")
            with self.assertRaisesRegex(ValueError, "missing ID"):
                preflight(dataset)
            pq.write_table(pa.table({"qid": [2], "pid": [1]}), dataset / "qrels.parquet")
            pq.write_table(pa.table({"id": [1], "text": [" "]}), dataset / "corpus.parquet")
            with self.assertRaisesRegex(ValueError, "blank text"):
                preflight(dataset)

    def test_rejects_blank_text_and_limit(self) -> None:
        with self.assertRaisesRegex(ValueError, "configured evaluation limit"):
            preflight(BENCHMARKS / "synthetic-zh-v1", max_questions=1)


if __name__ == "__main__":
    unittest.main()
