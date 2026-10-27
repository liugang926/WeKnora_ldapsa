#!/usr/bin/env bash
# Local root operator, or installed updater only. Never accepts shell content from a release.
set -Eeuo pipefail
umask 077
base=/srv/weknora-production
data=/data/weknora-production
release=$(realpath "${1:?release directory required}")
[[ "$release" == "$base/releases/"* && -f "$release/images.env" ]]
install -d -m 0755 "$data/runtime/docreader"
exec 9>"$base/deploy.lock"
flock -n 9 || exit 0
old=$(readlink -f "$base/current" || true)
compose() { docker compose --env-file "$base/secrets/app.env" --env-file "$1/images.env" -f "$1/production/compose.yaml" "${@:2}"; }
if [[ -e "$base/ACTIVATION_PENDING" ]]; then
  touch "$base/UPDATE_FAILED"
  compose "$release" stop || true
  echo 'Interrupted activation requires operator recovery; original backup pointer retained.' >&2
  exit 1
fi
protocol=$(python3 "$base/bin/retention.py" protocol "$release" --lock-fd 9)
compose "$release" config --quiet
for ref in $(cut -d= -f2 "$release/images.env"); do docker image inspect "$ref" >/dev/null; done
if [[ "$old" == "$release" ]]; then exit 0; fi
backup="$data/backups/$(date -u +%Y%m%dT%H%M%SZ)"
required=$(du -sb "$data/runtime" | cut -f1)
available=$(df -B1 --output=avail "$data" | tail -1 | tr -d " ")
(( available > required + 5*1024*1024*1024 )) || { echo "Insufficient backup space" >&2; exit 1; }
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
if ! cp -a "$base/secrets" "$backup/secrets"; then
  [[ ! -f "$old/production/compose.yaml" ]] || compose "$old" up -d --pull never
  exit 1
fi
[[ ! -f "$old/images.env" ]] || cp "$old/images.env" "$backup/previous-images.env"
printf '%s\n' "$old" > "$backup/previous-release"
sha256sum "$backup/runtime.tar.gz" > "$backup/SHA256SUMS"
rollback() {
  trap - ERR
  if [[ "$protocol" == journal ]]; then
    # The external purge ledger/pin may now be newer than any physical backup.
    # Never roll the DB back and reopen an old binary against that anchor.
    compose "$release" stop --timeout 90 || true
    touch "$base/UPDATE_FAILED"
    echo "Journal activation failed; stack closed. Pending pointer, runtime and backup retained: $backup" >&2
    exit 1
  fi
  compose "$release" down --timeout 90 || true
  mv "$data/runtime" "$backup/failed-runtime"
  tar -xzf "$backup/runtime.tar.gz" -C "$data"
  if [[ -f "$old/production/compose.yaml" ]]; then
    compose "$old" up -d --pull never --wait --wait-timeout 420 || true
  fi
  python3 -c 'import pathlib; pathlib.Path("/srv/weknora-production/ACTIVATION_PENDING").unlink(missing_ok=True)'
  touch "$base/UPDATE_FAILED"
  echo "Activation failed; restored pre-update data. Automatic updates paused. Backup: $backup" >&2
  exit 1
}
# Persist the recovery pointer before any new binary can migrate the DB.
python3 - "$base/ACTIVATION_PENDING" "$release" "$backup" <<'PYMARK'
import json, os, sys
path, release, backup = sys.argv[1:]
with open(path, 'w') as f:
    json.dump({'release': release, 'backup': backup}, f)
    f.flush(); os.fsync(f.fileno())
fd = os.open(os.path.dirname(path), os.O_DIRECTORY)
os.fsync(fd); os.close(fd)
PYMARK
trap rollback ERR
compose "$release" up -d --pull never --wait --wait-timeout 420 postgres redis docreader
if [[ "$protocol" == journal ]]; then
  python3 "$base/bin/retention.py" prepare "$release" --lock-fd 9
fi
compose "$release" up -d --pull never --wait --wait-timeout 420 app
compose "$release" exec -T app curl -fsS http://localhost:8080/health/readiness >/dev/null
compose "$release" exec -T app curl -fsS http://localhost:8080/api/v1/auth/config >/dev/null
bash "$base/bin/check-schema.sh" "$release"
if [[ "$protocol" == journal ]]; then
  python3 "$base/bin/retention.py" status "$release" --lock-fd 9
fi
# No automatic destructive rollback after reopening traffic.
if [[ "$protocol" != journal ]]; then trap - ERR; fi
ln -sfn "$release" "$base/current.new"
mv -Tf "$base/current.new" "$base/current"
python3 - <<'PYCOMMIT'
import os, pathlib
base = pathlib.Path('/srv/weknora-production')
fd = os.open(base, os.O_DIRECTORY)
os.fsync(fd)
os.close(fd)
PYCOMMIT
if ! compose "$release" up -d --pull never --wait --wait-timeout 180 frontend; then
  if [[ "$protocol" == journal ]]; then rollback; fi
  touch "$base/UPDATE_FAILED"
  echo 'Frontend failed; backend and data retained for operator recovery.' >&2
  exit 1
fi
python3 - "$release" <<'PYCLEAR'
import os, pathlib, sys
base = pathlib.Path('/srv/weknora-production')
with (base/'active-release.new').open('w') as f:
    f.write(pathlib.Path(sys.argv[1]).name + '\n')
    f.flush(); os.fsync(f.fileno())
os.replace(base/'active-release.new', base/'active-release')
fd = os.open(base, os.O_DIRECTORY)
os.fsync(fd)
(base/'ACTIVATION_PENDING').unlink()
os.fsync(fd); os.close(fd)
PYCLEAR
trap - ERR
echo "Activated $(basename "$release"); backup $backup"
