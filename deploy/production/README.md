# WeKnora production on 10.106.1.128

Public URL: https://ai-doc.tianjizn.com. The operator's reverse proxy terminates
TLS and forwards to http://10.106.1.128:18080. Preserve Host, enable SSE without
buffering, allow 50 MB uploads, and use a 3600-second read timeout.

## Release contract

Merge reviewed changes to `liugang926/WeKnora_ldapsa` main first. Then create and
push a unique tag such as `prod-20261018.1` on that merged commit. The
`Production release` workflow rejects tags outside main, builds Linux amd64
app (including AnyDoc) and UI, and publishes a complete GitHub release only
after all image bundles and SHA-256 manifests have uploaded. A normal branch
push, draft release or prerelease does not deploy. Never reuse a production tag.

Published image identities and configuration digests are preserved in the release
manifest. The server verifies their configuration and filesystem layers before
recording its own immutable image IDs in `resolved-images.json`; this supports
Docker classic and containerd stores without trusting a mutable tag.

The server polls published `prod-*` releases every five minutes. It downloads
and verifies complete bundles, loads exact image IDs, stops this stack for a
consistent backup, starts database/Redis/app, verifies health, the exact clean migration version
and application tables,
and only then opens the frontend. This is a maintenance-window deployment,
not zero-downtime. Future schema migrations run through the application's normal
AUTO_MIGRATE path. Production releases require local/PR tests and review before
tagging; image compilation does not replace behavioral acceptance.

The production server does not need a GitHub token, inbound GitHub webhook,
build toolchain, Docker Hub access, or a self-hosted Actions runner. The fork is
public; release assets contain code/images only, never runtime secrets/data.
Downloads use this public repository's GitHub API asset endpoints with `Accept: application/octet-stream`, without a token.

## First installation

As root, run `install.sh` from a reviewed copy of this directory. It refuses
to overwrite an existing environment. Run the update service once, then run
`bootstrap.py` while the frontend still binds only to loopback. It creates a
random-password administrator, closes public registration and checks login.
Verify system-administrator promotion before changing BIND_IP to 10.106.1.128
and recreating the frontend. Credentials live in root-only
`/srv/weknora-production/secrets/admin.json`; rotate through the application.
Enable the updater with `systemctl enable --now weknora-production-update.timer`
only after acceptance. Enable and start `weknora-production.service` after the
first successful deployment. It restores the current stack on every boot,
requires Docker, the resource slice and the data mount, and serializes against
updates. Containers also retain their restart policies. Image downloads use eight bounded HTTP ranges with retained chunk caches;
metadata uses bounded atomic reads. Every complete asset must pass SHA-256 verification. No development data is imported.

## Updating deployment infrastructure

Production releases update application images and the release's Compose/nginx
configuration. The trusted helpers installed in `/srv/weknora-production/bin`
and the systemd units in `/etc/systemd/system` are separate operator-managed
copies; publishing a tag does not replace them.

When reviewed changes affect these helpers or units, stop the update timer and
wait for any running updater to finish. As root, hold the deployment lock while
copying the affected helpers from the reviewed source into `bin`, and synchronize
the affected unit definitions. The update service, timer and resource slice are
defined in `install.sh`; the application unit is `weknora-production.service`.
Run `systemctl daemon-reload` after unit changes, verify the effective units, and
re-enable the timer with `systemctl enable --now weknora-production-update.timer`.
Both application startup and the updater require the deployment and data mounts.

`install.sh` is for first installation only and refuses an initialized instance.
Do not rerun it to upgrade infrastructure or regenerate runtime secrets. Copy
only the reviewed changes, retaining the existing environment and data.

## Boundaries and resource allocation

- Four containers: app, frontend, ParadeDB/PostgreSQL 17, Redis with persistent AOF.
- AnyDoc runs inside the app; the optional Python DocReader, OCR/VLM, graph,
  sandbox and local model containers are not installed. Configure supported
  document engines and remote chat/embedding models in the administrator UI.
- No GPU devices or Docker socket mounts. Aggregate slice: 6 CPU equivalents,
  MemoryHigh 28 GiB, MemoryMax 32 GiB, swap disabled. Individual limits also apply.
- Runtime data: `/data/weknora-production/runtime`; deployment and secrets:
  `/srv/weknora-production`; logs rotate at 3 x 10 MB per container.
- Dedicated network 172.30.54.0/24; database/Redis/app have no published ports.
- Host observed: 48 logical CPUs, 251 GiB RAM, healthy RAID10 `/data` with ~6.2 TB
  available. Reserve at least 64 GiB RAM and an initial 2 TB planning allowance
  for future Nextcloud. These are planning reserves, not filesystem quotas.
- Existing Qwen models, ports 30443/30444 and inactive LiteLLM migration data
  are retained. Their measured health is checked before and after deployment.

## Recovery and operation

`systemctl status weknora-production-update.timer`
`journalctl -u weknora-production-update.service`

Backups include the cold PostgreSQL/Redis/file runtime plus runtime secrets,
under `/data/weknora-production/backups`. They are on the same RAID, so separate
off-host backup is still required for host-loss recovery. No backups or old
images are automatically deleted. Monitor free space; download refuses below
30 GiB and activation should have enough space for a complete runtime backup.
Large data sets need a dedicated snapshot/backup strategy before scaling.

A backend startup failure restores the pre-update physical data snapshot and
old images while the frontend is closed, then creates `UPDATE_FAILED`. The
updater stays paused until an operator investigates and removes that marker.
An interrupted activation leaves a durable `ACTIVATION_PENDING` pointer to its
backup. Boot closes the stack rather than starting an old binary on a database
that may have migrated; an operator must complete the recorded recovery.
If the frontend fails after backend activation, data is retained rather than
performing a destructive rollback after traffic may have reopened. For manual
rollback, stop the stack and restore the matching backup and prior release;
never run an old binary against an unverified newer database schema. Do not use
`docker compose down -v` or broad Docker cleanup commands.
