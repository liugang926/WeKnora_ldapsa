"""Validate a fully synthetic benchmark manifest and write WeKnora Parquet files.

Only use this on data that has separately been approved for the target test
environment. The bundled synthetic-zh-v1 fixture contains no real identities.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from fixture_access import persona_scopes


def write_rows(path: Path, fields: list[tuple[str, pa.DataType]], rows: list[dict]) -> None:
    schema = pa.schema(fields)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path, compression="zstd")


def generate(manifest_path: Path) -> None:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    documents = manifest["documents"]
    cases = manifest["cases"]
    doc_ids = {item["id"] for item in documents}
    case_ids = {item["id"] for item in cases}
    if len(doc_ids) != len(documents) or len(case_ids) != len(cases):
        raise ValueError("duplicate document or case ID")
    if not documents or not cases:
        raise ValueError("empty benchmark")
    if any(not item["text"].strip() for item in documents):
        raise ValueError("blank passage")
    if any(not item["question"].strip() for item in cases):
        raise ValueError("blank question")
    personas = persona_scopes(manifest)
    scopes_by_id = {item["id"]: item["scope"] for item in documents}
    for case in cases:
        if not set(case["evidence"]).issubset(doc_ids):
            raise ValueError(f"case {case['id']} references missing evidence")
        if case.get("persona") not in (None, *personas):
            raise ValueError(f"case {case['id']} has unknown persona")
        allowed_scopes = personas.get(case.get("persona"), personas.get("employee", []))
        if case.get("category") not in ("permission_denied", "no_answer") and any(
            scopes_by_id[doc_id] not in allowed_scopes for doc_id in case["evidence"]
        ):
            raise ValueError(f"case {case['id']} evidence is not visible to persona")
        forbidden = case.get("must_not_access", [])
        if not set(forbidden).issubset(doc_ids):
            raise ValueError(f"case {case['id']} references missing forbidden passage")
        if case.get("category") == "permission_denied" and (
            not forbidden or any(scopes_by_id[doc_id] in allowed_scopes for doc_id in forbidden)
        ):
            raise ValueError(f"case {case['id']} has an invalid forbidden scope")
        if case.get("category") in ("no_answer", "permission_denied"):
            if case["answer"] or case["evidence"]:
                raise ValueError(f"case {case['id']} must not have an answer")
        elif not case["answer"].strip() or not case["evidence"]:
            raise ValueError(f"case {case['id']} needs an answer and evidence")

    output = manifest_path.parent
    text_fields = [("id", pa.int64()), ("text", pa.string())]
    write_rows(output / "corpus.parquet", text_fields,
               [{"id": item["id"], "text": item["text"]} for item in documents])
    write_rows(output / "queries.parquet", text_fields,
               [{"id": item["id"], "text": item["question"]} for item in cases])
    write_rows(output / "answers.parquet", text_fields,
               [{"id": item["id"], "text": item["answer"]}
                for item in cases if item["answer"]])
    write_rows(output / "qrels.parquet", [("qid", pa.int64()), ("pid", pa.int64())],
               [{"qid": item["id"], "pid": pid}
                for item in cases for pid in item["evidence"]])
    write_rows(output / "qas.parquet", [("qid", pa.int64()), ("aid", pa.int64())],
               [{"qid": item["id"], "aid": item["id"]}
                for item in cases if item["answer"]])
    print(f"Generated {len(cases)} cases and {len(documents)} passages in {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    generate(parser.parse_args().manifest)
