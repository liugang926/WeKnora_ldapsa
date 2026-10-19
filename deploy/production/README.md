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
consistent backup, starts database/Redis, runs the packaged offline journal migration
and initialization/verification gate, then starts app, verifies health, the exact clean migration version
and application tables,
and only then opens the frontend. This is a maintenance-window deployment,
not zero-downtime. Protocol 2 releases declare `original_body_journal` in
`required_features`; their migrations run through the reviewed stable helper and
packaged CLI with all application readers stopped. The app has `AUTO_MIGRATE=false`.
Production releases require local/PR tests and review before
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
updates. App/frontend use `on-failure:5` for bounded crash recovery; this policy
does not restart containers when the Docker daemon restarts. The application unit
has `PartOf=docker.service`, so a controlled Docker restart reruns the same gate.
Database/Redis retain their own restart policies. Image downloads use eight bounded HTTP ranges with retained chunk caches;
metadata uses bounded atomic reads. Every complete asset must pass SHA-256 verification. No development data is imported.

For the first protocol 2 upgrade, pause the update timer and wait for the active
updater to finish. Run `python3 /srv/weknora-production/bin/update.py --stage-only`
to verify/load the release without starting services or changing the live database.
Restore a fresh production schema 132 dump into an isolated PostgreSQL instance,
use a separate network and separate runtime/journal/key/pin directories, and
exercise migration, first initialization, verification, restart and failed-restore
rejection there. Never point a rehearsal at production's runtime or anchor paths.
Only after the rehearsal and infrastructure review should the timer be re-enabled
to perform the real automatic upgrade during its maintenance window.

## LDAP / Active Directory setup

Production uses `LDAP_CONFIG_SOURCE=database` and `LDAP_DIRECTORY_ID=tianji-ad`
so a system administrator can edit the LDAP/AD page at `/platform/settings?section=directory`. Without this setting,
the application defaults to file management and locks all connection fields.
For an existing installation, back up the root-only environment, set these values
in `/srv/weknora-production/secrets/app.env`, retain the intended directory ID,
and recreate only the app service using the current release's Compose file. Retain the existing encryption key.

A fresh database has no AD connection. Fill in the real domain controllers,
search scope, service account and trusted enterprise CA, test the connection,
then enable it. The login page shows domain login only after it is enabled.
Do not enable an empty or synthetic directory. The credential is encrypted in
the production database and is covered by the existing cold backup procedure.
An enterprise CA file must be readable inside the app container; for example,
an operator can place a public CA certificate under the existing files mount at
`/data/weknora-production/runtime/files/.ldap-ca/ad-ca.pem` and enter
`/data/files/.ldap-ca/ad-ca.pem` in the form. Use a dedicated operator-owned
subdirectory and certificate permissions. No AD data or credentials are imported by
the installer. LDAP readiness is separate from ordinary HTTP health: acceptance
reports `ldap_runtime=NOT_CONFIGURED` until AD is enabled, and requires an enabled
directory to have an available, unpaused synchronized snapshot.

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
Protocol 2 additionally requires the reviewed `retention.py`, `activate.sh` and
`start.sh` in `bin`, and the updated application unit/restart policies. Publishing
an application tag cannot install these trusted helpers. Before enabling protocol 2,
verify that Docker restart propagation uses the updated application unit.

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
- External original-body retention anchor: `/data/weknora-production/retention`.
  The separate `journal`, `key`, and `pin` subdirectories are bind-mounted at
  `/data/body-journal`, `/data/body-key` (read-only), and `/data/body-pin`.
  They are outside runtime backups. The stable helper resolves the new image's
  `appuser` UID/GID, creates directories as 0700 and files as 0600, and refuses
  symlinks, wrong owners, permissive modes or files with multiple hard links.
  It never prints the random HMAC key or a database password.
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

For journal-enabled releases, any activation failure closes the stack and keeps
the current runtime, `ACTIVATION_PENDING`, `UPDATE_FAILED` and its recorded backup.
It never automatically restores an older database and starts an old app. The
external purge ledger/pin may be ahead of a database snapshot. Legacy automatic
rollback applies only before a retention anchor has ever been established; a
legacy release is rejected once anchor material exists. The updater stays paused
until an operator completes recovery and clears the failure marker.
An interrupted activation leaves a durable `ACTIVATION_PENDING` pointer to its
backup. Boot closes the stack rather than starting an old binary on a database
that may have migrated. Every normal boot runs offline `verify` with app/frontend
stopped, then rechecks the exact clean schema before opening the frontend. Boot
never initializes or reconciles an anchor. A root-only verification receipt is
bound to the current boot, release, immutable image, appuser and key/journal identity.
`retention.py status <installed-release-directory>` performs read-only running
acceptance against that receipt and the app's actual mounts/environment/health;
it does not execute the offline CLI while writers are active.

For operator recovery, stop the update timer, hold `/srv/weknora-production/deploy.lock`,
and stop every app, reader and worker that can access the database, including any
operator-created maintenance containers. Keep the latest external `retention`
directory intact while selecting a matching runtime backup and a journal-capable
release. Preserve the failed runtime for investigation. If a physical database
restore is necessary, restore runtime only: never roll the journal/key/pin back
with it. With PostgreSQL/Redis running and all readers stopped, explicitly run:

```sh
python3 /srv/weknora-production/bin/retention.py operator-reconcile /srv/weknora-production/releases/PROD_TAG --lock-fd 9
python3 /srv/weknora-production/bin/retention.py verify /srv/weknora-production/releases/PROD_TAG --lock-fd 9
```

Here descriptor 9 must be the already-held deployment lock (for example,
`exec 9>/srv/weknora-production/deploy.lock; flock -x 9` in the operator shell).
Without `--lock-fd`, the helper acquires the lock itself. Reconcile replays only
authenticated external purge events compatible with the restored original body
metadata; it never invents leases or clears mismatched retention state. A refusal
means keep service closed and choose an appropriate recovery point/investigate.
The helper does not reset or delete key/ledger/pin, clear deployment markers,
switch the selected release, or start traffic. An operator must verify the correct
release/schema and complete any publication recovery before atomically selecting
that release and clearing the recorded pending/failure markers under the lock.
Then release the lock and start `weknora-production.service`; startup verifies again
before opening traffic. Do not restore a pre-journal binary after an anchor exists.

Back up the external anchor securely and independently from runtime, with its
latest sequence preserved. Same-host copies do not provide host-loss recovery;
restoring a stale pin/key/ledger along with a database defeats the external rollback
boundary. Do not delete partial initialization files to retry `init`: the helper's
root-only provisioning latch and the CLI's virgin-database checks intentionally
refuse ambiguous or regressed state. Do not use
`docker compose down -v` or broad Docker cleanup commands.

Deployment acceptance checks the declared feature gates, journal `status`, that
the onboarding endpoint rejects a normal web JWT, the file-status v2 route and
the GC management API. These checks establish deployed contracts; real Nextcloud
source pairing and end-to-end document synchronization still require separate
business acceptance. `/source-knowledge` is a scoped-only entry point. Capacity
and GC controls are under knowledge-base settings → data sources (capacity needs
an existing Nextcloud source). There is no separate Nextcloud integration settings
tab. Configure models in the administrator UI as requested, and retain the user's
HTTPS reverse proxy; do not enable a development HTTP bypass for pairing.
