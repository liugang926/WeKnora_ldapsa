#!/usr/bin/env bash
# Restore the selected production stack at boot, serialized with deployments.
set -Eeuo pipefail
umask 077
base=/srv/weknora-production
data=/data/weknora-production
exec 9>"$base/deploy.lock"
flock 9
release=$(readlink -f "$base/current")
[[ "$release" == "$base/releases/"* && -f "$release/images.env" ]]
install -d -m 0755 "$data/runtime/docreader"
compose() { docker compose --env-file "$base/secrets/app.env" --env-file "$release/images.env" -f "$release/production/compose.yaml" "$@"; }
fail_closed() {
  trap - ERR
  compose stop --timeout 90 app frontend docreader || true
  touch "$base/UPDATE_FAILED"
  echo 'Startup verification failed; application and traffic remain closed.' >&2
  exit 1
}
trap fail_closed ERR
if [[ -e "$base/ACTIVATION_PENDING" ]]; then
  # A power loss may leave new-schema data with the previous current symlink.
  compose stop
  touch "$base/UPDATE_FAILED"
  echo 'Interrupted activation requires recovery from its recorded backup; startup closed.' >&2
  exit 1
fi
compose stop frontend app docreader
protocol=$(python3 "$base/bin/retention.py" protocol "$release" --lock-fd 9)
compose up -d --pull never --wait --wait-timeout 420 postgres redis docreader
if [[ "$protocol" == journal ]]; then
  # Boot can verify an existing anchor only; never migrate, init or reconcile.
  python3 "$base/bin/retention.py" verify "$release" --lock-fd 9
fi
compose up -d --pull never --wait --wait-timeout 420 app
bash "$base/bin/check-schema.sh" "$release"
if [[ "$protocol" == journal ]]; then
  python3 "$base/bin/retention.py" status "$release" --lock-fd 9
fi
compose up -d --pull never --wait --wait-timeout 180 frontend
trap - ERR
