"""Validate a local WeKnora evaluation dataset without contacting any service.

The report contains only counts and file hashes. It is not a de-identification
review, model-use approval, or permission test.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


DATASET_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
SCHEMAS = {
    "corpus": (("id", pa.int64()), ("text", pa.string())),
    "queries": (("id", pa.int64()), ("text", pa.string())),
    "answers": (("id", pa.int64()), ("text", pa.string())),
    "qrels": (("qid", pa.int64()), ("pid", pa.int64())),
    "qas": (("qid", pa.int64()), ("aid", pa.int64())),
}


def _read_table(path: Path, fields: tuple) -> pa.Table:
    table = pq.read_table(path)
    if table.schema.names != [name for name, _ in fields]:
        raise ValueError(f"{path.name}: unexpected columns")
    for name, expected_type in fields:
        column = table[name]
        if column.type != expected_type or column.null_count:
            raise ValueError(f"{path.name}: invalid type or null in {name}")
    return table


def _ids(table: pa.Table, name: str, label: str) -> set[int]:
    values = table[name].to_pylist()
    if len(values) != len(set(values)):
        raise ValueError(f"duplicate {label} ID")
    return set(values)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def preflight(directory: Path, max_questions: int = 100) -> dict:
    """Return content-free metadata after checking the same key relations as Go."""
    if not DATASET_ID.fullmatch(directory.name):
        raise ValueError("invalid evaluation dataset ID")
    if not 1 <= max_questions <= 10000:
        raise ValueError("max_questions must be between 1 and 10000")
    if not directory.is_dir():
        raise ValueError("dataset directory does not exist")

    tables = {name: _read_table(directory / f"{name}.parquet", fields)
              for name, fields in SCHEMAS.items()}
    corpus_ids = _ids(tables["corpus"], "id", "passage")
    query_ids = _ids(tables["queries"], "id", "question")
    answer_ids = _ids(tables["answers"], "id", "answer")
    if not corpus_ids or not query_ids:
        raise ValueError("dataset requires passages and questions")
    if len(query_ids) > max_questions:
        raise ValueError("question count exceeds configured evaluation limit")
    for name in ("corpus", "queries", "answers"):
        if any(not value.strip() for value in tables[name]["text"].to_pylist()):
            raise ValueError(f"{name}.parquet contains blank text")

    qrels = list(zip(tables["qrels"]["qid"].to_pylist(),
                     tables["qrels"]["pid"].to_pylist()))
    if len(qrels) != len(set(qrels)):
        raise ValueError("duplicate question/evidence relation")
    if any(qid not in query_ids or pid not in corpus_ids for qid, pid in qrels):
        raise ValueError("question/evidence relation references a missing ID")

    qas = list(zip(tables["qas"]["qid"].to_pylist(),
                   tables["qas"]["aid"].to_pylist()))
    if len(qas) != len({qid for qid, _ in qas}):
        raise ValueError("duplicate question/answer relation")
    if any(qid not in query_ids or aid not in answer_ids for qid, aid in qas):
        raise ValueError("question/answer relation references a missing ID")

    return {
        "dataset_id": directory.name,
        "counts": {name: table.num_rows for name, table in tables.items()},
        "file_sha256": {f"{name}.parquet": _sha256(directory / f"{name}.parquet")
                        for name in SCHEMAS},
        "note": "Structure only; de-identification, use approval, evidence quality, and AD permissions are not verified.",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="local <dataset_id> directory")
    parser.add_argument("--max-questions", type=int, default=100,
                        help="match EVALUATION_MAX_QUESTIONS on the app (default: 100)")
    args = parser.parse_args()
    print(json.dumps(preflight(args.directory, args.max_questions),
                     ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
