from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq

from fixture_access import persona_scopes
from generate_fixture import generate


SOURCE = Path(__file__).resolve().parents[2] / "dataset/benchmarks/synthetic-zh-v1/fixture.json"
ENTERPRISE_SOURCE = Path(__file__).resolve().parents[2] / "dataset/benchmarks/synthetic-enterprise-zh-v2/fixture.json"


class FixtureTests(unittest.TestCase):
    def assert_committed_parquet_matches_manifest(self, source: Path) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            path.write_bytes(source.read_bytes())
            generate(path)
            for name in ("corpus", "queries", "answers", "qrels", "qas"):
                self.assertTrue(
                    pq.read_table(path.parent / f"{name}.parquet").equals(
                        pq.read_table(source.parent / f"{name}.parquet")
                    ),
                    f"committed {source.parent.name}/{name}.parquet differs from fixture.json",
                )

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
        self.assert_committed_parquet_matches_manifest(SOURCE)

    def test_rejects_dangling_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            fixture = json.loads(SOURCE.read_text(encoding="utf-8"))
            fixture["cases"][0]["evidence"] = [999999]
            path.write_text(json.dumps(fixture), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "missing evidence"):
                generate(path)

    def test_enterprise_fixture_categories_and_nested_groups(self) -> None:
        fixture = json.loads(ENTERPRISE_SOURCE.read_text(encoding="utf-8"))
        self.assertEqual(len(fixture["documents"]), 42)
        self.assertEqual(len(fixture["cases"]), 70)
        self.assertEqual(
            {category: sum(case["category"] == category for case in fixture["cases"])
             for category in {case["category"] for case in fixture["cases"]}},
            {"zh": 12, "cross_document": 8, "no_answer": 10,
             "permission_allowed": 20, "permission_denied": 20},
        )
        scopes = persona_scopes(fixture)
        for department in ("operations", "finance", "hr", "procurement", "security"):
            self.assertEqual(scopes[f"{department}_direct"], ["all", department])
            self.assertEqual(scopes[f"{department}_nested"], ["all", department])
        self.assertEqual(scopes["employee"], ["all"])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            path.write_bytes(ENTERPRISE_SOURCE.read_bytes())
            generate(path)
            self.assertEqual(pq.read_table(Path(directory) / "corpus.parquet").num_rows, 42)
            self.assertEqual(pq.read_table(Path(directory) / "queries.parquet").num_rows, 70)
            self.assertEqual(pq.read_table(Path(directory) / "answers.parquet").num_rows, 40)
            self.assertEqual(pq.read_table(Path(directory) / "qas.parquet").num_rows, 40)
        self.assert_committed_parquet_matches_manifest(ENTERPRISE_SOURCE)

    def test_rejects_synthetic_group_cycle(self) -> None:
        fixture = json.loads(ENTERPRISE_SOURCE.read_text(encoding="utf-8"))
        fixture["access_model"]["groups"]["ops_staff"]["parent_groups"] = ["ops_oncall"]
        with self.assertRaisesRegex(ValueError, "cycle"):
            persona_scopes(fixture)

    def test_rejects_invisible_answer_evidence(self) -> None:
        fixture = json.loads(ENTERPRISE_SOURCE.read_text(encoding="utf-8"))
        fixture["cases"][0]["evidence"] = [1101]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            path.write_text(json.dumps(fixture), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "not visible"):
                generate(path)


if __name__ == "__main__":
    unittest.main()
