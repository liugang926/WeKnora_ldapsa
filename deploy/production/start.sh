#!/usr/bin/env bash
# Restore the selected production stack at boot, serialized with deployments.
set -euo pipefail
base=/srv/weknora-production
exec 9>"$base/deploy.lock"
flock 9
release=$(readlink -f "$base/current")
[[ "$release" == "$base/releases/"* && -f "$release/images.env" ]]
compose() { docker compose --env-file "$base/secrets/app.env" --env-file "$release/images.env" -f "$release/production/compose.yaml" "$@"; }
if [[ -e "$base/ACTIVATION_PENDING" ]]; then
  # A power loss may leave new-schema data with the previous current symlink.
  compose stop
  touch "$base/UPDATE_FAILED"
  echo 'Interrupted activation requires recovery from its recorded backup; startup closed.' >&2
  exit 1
fi
compose stop frontend
compose up -d --pull never --wait --wait-timeout 420 postgres redis app
bash "$base/bin/check-schema.sh" "$release"
compose up -d --pull never --wait --wait-timeout 180 frontend
