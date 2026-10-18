#!/usr/bin/env bash
# A live HTTP process is insufficient: migrations are best-effort in the app.
set -euo pipefail
base=/srv/weknora-production
release=$(realpath "${1:?release directory required}")
compose() { docker compose --env-file "$base/secrets/app.env" --env-file "$release/images.env" -f "$release/production/compose.yaml" "$@"; }
expected=$(compose exec -T app sh -c 'find /app/migrations/versioned -name "*.up.sql" -printf "%f\n" | cut -d_ -f1 | sort -n | tail -1' | tr -d '\r\n')
[[ "$expected" =~ ^[0-9]+$ ]]
actual=$(compose exec -T postgres psql -X -At -v ON_ERROR_STOP=1 -U weknora -d weknora -c "SELECT version::text || '|' || dirty::text FROM schema_migrations" | tr -d '\r\n')
[[ "$actual" == "$((10#$expected))|false" ]] || { echo "Schema gate failed: expected $((10#$expected))|false, got $actual" >&2; exit 1; }
compose exec -T postgres psql -X -At -v ON_ERROR_STOP=1 -U weknora -d weknora -c "SELECT 1 FROM users LIMIT 0; SELECT 1 FROM knowledge_bases LIMIT 0; SELECT 1 FROM models LIMIT 0;" >/dev/null
echo "Schema $((10#$expected)) clean and application tables readable"
