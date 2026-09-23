from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq

from generate_fixture import generate


SOURCE = Path(__file__).resolve().parents[2] / "dataset/benchmarks/synthetic-zh-v1/fixture.json"


class FixtureTests(unittest.TestCase):
    def test_generates_complete_benchmark(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            path.write_bytes(SOURCE.read_bytes())
            generate(path)
            self.assertEqual(pq.read_table(Path(directory) / "corpus.parquet").num_rows, 16)
            self.assertEqual(pq.read_table(Path(directory) / "queries.parquet").num_rows, 16)
            self.assertEqual(pq.read_table(Path(directory) / "answers.parquet").num_rows, 12)
            self.assertEqual(pq.read_table(Path(directory) / "qrels.parquet").num_rows, 18)
            self.assertEqual(pq.read_table(Path(directory) / "qas.parquet").num_rows, 12)

    def test_rejects_dangling_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            fixture = json.loads(SOURCE.read_text(encoding="utf-8"))
            fixture["cases"][0]["evidence"] = [999999]
            path.write_text(json.dumps(fixture), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "missing evidence"):
                generate(path)


if __name__ == "__main__":
    unittest.main()
