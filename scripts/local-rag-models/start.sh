#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${RAG_MODEL_TOKEN_FILE:-}" || ! -f "$RAG_MODEL_TOKEN_FILE" ]]; then
  echo "Set RAG_MODEL_TOKEN_FILE to a mode-0600 token file" >&2
  exit 1
fi
if [[ "$(stat -f %Lp "$RAG_MODEL_TOKEN_FILE")" != "600" ]]; then
  echo "RAG_MODEL_TOKEN_FILE must have mode 0600" >&2
  exit 1
fi
if [[ -z "${RAG_MODEL_PYTHON:-}" || ! -x "$RAG_MODEL_PYTHON" ]]; then
  echo "Set RAG_MODEL_PYTHON to the model virtualenv's Python" >&2
  exit 1
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export RAG_MODEL_TOKEN="$(tr -d '\r\n' < "$RAG_MODEL_TOKEN_FILE")"
export HF_HUB_OFFLINE="${HF_HUB_OFFLINE:-1}"
exec "$RAG_MODEL_PYTHON" "$script_dir/server.py"
