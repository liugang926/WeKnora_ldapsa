#!/usr/bin/env bash
set -euo pipefail
umask 077
[[ $(id -u) == 0 ]]
base=/srv/weknora-production
data=/data/weknora-production
[[ ! -e "$base/secrets/app.env" ]] || { echo 'Already initialized; refusing to overwrite secrets'; exit 1; }
install -d -m 700 "$base/secrets" "$base/bin" "$base/releases" "$data/runtime" "$data/backups" "$data/downloads"
install -d "$data/runtime/"{postgres,redis,files}
cp "$(dirname "$0")/"{activate.sh,start.sh,check-schema.sh,update.py,parallel_download.py,validate_release.py} "$base/bin/"
python3 - <<'PY'
import pathlib, secrets
base=pathlib.Path('/srv/weknora-production/secrets')
redis=secrets.token_hex(32)
values=dict(DB_DRIVER='postgres',DB_HOST='postgres',DB_PORT='5432',DB_USER='weknora',DB_NAME='weknora',DB_PASSWORD=secrets.token_hex(32),
    REDIS_ADDR='redis:6379',REDIS_PASSWORD=redis,REDIS_DB='0',REDIS_PREFIX='stream:',STREAM_MANAGER_TYPE='redis',
    RETRIEVE_DRIVER='postgres',STORAGE_TYPE='local',LOCAL_STORAGE_BASE_DIR='/data/files',
    SYSTEM_AES_KEY=secrets.token_hex(16),SYSTEM_SIGNING_KEY=secrets.token_hex(32),JWT_SECRET=secrets.token_hex(32),
    LOG_LEVEL='info',GIN_MODE='release',TZ='Asia/Shanghai',GOMAXPROCS='3',CONCURRENCY_POOL_SIZE='2',AUTO_MIGRATE='true',AUTO_RECOVER_DIRTY='false',
    DOCREADER_ADDR='',OLLAMA_OPTIONAL='true',WEKNORA_SANDBOX_DOCKER_ENABLED='false',
    DISABLE_REGISTRATION='false',WEKNORA_AUTH_COMPLEX_PASSWORD_ENABLED='true',
    WEKNORA_BOOTSTRAP_SYSTEM_ADMIN_EMAIL='admin@ai-doc.tianjizn.com',
    FRONTEND_BASE_URL='https://ai-doc.tianjizn.com',APP_EXTERNAL_URL='https://ai-doc.tianjizn.com',
    WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS='https://cloud.tianjizn.com',
    WEKNORA_TRUSTED_PROXIES='172.30.54.0/24',BIND_IP='127.0.0.1',HTTP_PORT='18080')
(base/'app.env').write_text(''.join(k+'='+v+'\n' for k,v in values.items()))
(base/'redis.conf').write_text('appendonly yes\nmaxmemory 1536mb\nmaxmemory-policy noeviction\nrequirepass '+redis+'\n')
(base/'redis.conf').chmod(0o644) # Parent directory is root-only; redis container drops UID.
(base/'admin.json').write_text(__import__('json').dumps(dict(username='admin',email=values['WEKNORA_BOOTSTRAP_SYSTEM_ADMIN_EMAIL'],password=secrets.token_urlsafe(30)+'Aa1!')))
PY
cat > /etc/systemd/system/weknora-production.slice <<'UNIT'
[Unit]
Description=WeKnora production resource budget (no GPUs)
[Slice]
CPUQuota=600%
MemoryHigh=28G
MemoryMax=32G
MemorySwapMax=0
CPUWeight=50
IOWeight=50
UNIT
cat > /etc/systemd/system/weknora-production-update.service <<'UNIT'
[Unit]
Description=Pull verified WeKnora production release
After=network-online.target docker.service
Wants=network-online.target
Requires=docker.service
[Service]
Type=oneshot
ExecStart=/usr/bin/python3 /srv/weknora-production/bin/update.py
TimeoutStartSec=2h
Nice=10
IOSchedulingClass=best-effort
IOSchedulingPriority=7
CPUQuota=200%
MemoryMax=2G
UMask=0077
UNIT
cat > /etc/systemd/system/weknora-production-update.timer <<'UNIT'
[Unit]
Description=Check published WeKnora production versions every five minutes
[Timer]
OnBootSec=3min
OnUnitInactiveSec=5min
RandomizedDelaySec=30s
Persistent=true
[Install]
WantedBy=timers.target
UNIT
install -m 644 "$(dirname "$0")/weknora-production.service" /etc/systemd/system/weknora-production.service
systemctl daemon-reload
systemctl start weknora-production.slice
# Timer is enabled only after private bootstrap and acceptance.
echo 'Initialized. Run update service, bootstrap on loopback, then enable timer.'
