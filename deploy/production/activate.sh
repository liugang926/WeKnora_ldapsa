#!/usr/bin/env bash
# Local root operator, or installed updater only. Never accepts shell content from a release.
set -Eeuo pipefail
umask 077
base=/srv/weknora-production
data=/data/weknora-production
release=$(realpath "${1:?release directory required}")
[[ "$release" == "$base/releases/"* && -f "$release/images.env" ]]
exec 9>"$base/deploy.lock"
flock -n 9 || exit 0
old=$(readlink -f "$base/current" || true)
compose() { docker compose --env-file "$base/secrets/app.env" --env-file "$1/images.env" -f "$1/production/compose.yaml" "${@:2}"; }
compose "$release" config --quiet
for ref in $(cut -d= -f2 "$release/images.env"); do docker image inspect "$ref" >/dev/null; done
if [[ "$old" == "$release" ]]; then exit 0; fi
backup="$data/backups/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$backup"
# All writers are stopped for a consistent physical DB/Redis/file snapshot.
# This is deliberately a maintenance window; no unsafe old-binary/new-schema rollback.
if [[ -f "$old/production/compose.yaml" ]]; then
  compose "$old" stop
fi
if ! tar -czf "$backup/runtime.tar.gz" -C "$data" runtime; then
  [[ ! -f "$old/production/compose.yaml" ]] || compose "$old" up -d --pull never
  exit 1
fi
cp -a "$base/secrets" "$backup/secrets"
[[ ! -f "$old/images.env" ]] || cp "$old/images.env" "$backup/previous-images.env"
printf '%s\n' "$old" > "$backup/previous-release"
sha256sum "$backup/runtime.tar.gz" > "$backup/SHA256SUMS"
rollback() {
  trap - ERR
  compose "$release" down --timeout 90 || true
  mv "$data/runtime" "$backup/failed-runtime"
  tar -xzf "$backup/runtime.tar.gz" -C "$data"
  if [[ -f "$old/production/compose.yaml" ]]; then
    compose "$old" up -d --pull never --wait --wait-timeout 420 || true
  fi
  touch "$base/UPDATE_FAILED"
  echo "Activation failed; restored pre-update data. Automatic updates paused. Backup: $backup" >&2
  exit 1
}
trap rollback ERR
compose "$release" up -d --pull never --wait --wait-timeout 420 postgres redis app
compose "$release" exec -T app curl -fsS http://localhost:8080/health >/dev/null
compose "$release" exec -T app curl -fsS http://localhost:8080/api/v1/auth/config >/dev/null
# No automatic destructive rollback after reopening traffic.
trap - ERR
ln -sfn "$release" "$base/current.new"
mv -Tf "$base/current.new" "$base/current"
if ! compose "$release" up -d --pull never --wait --wait-timeout 180 frontend; then
  touch "$base/UPDATE_FAILED"
  echo 'Frontend failed; backend and data retained for operator recovery.' >&2
  exit 1
fi
printf '%s\n' "$(basename "$release")" > "$base/active-release"
echo "Activated $(basename "$release"); backup $backup"
