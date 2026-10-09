# Original body lifecycle and offline recovery

Migration PostgreSQL 152 / SQLite 71 separates exact original input bodies from their immutable origin metadata. Known historic producer columns contain only a versioned field/body reference; current searchable SQL fields remain inline and have exact body mirrors. Purge clears vault payloads and exact inactive inline fields, including only `knowledges.metadata.manual.content`, while preserving unrelated metadata and source pointers. Heads, revisions, hashes, source tuples, and purge receipts contain no original text. Retained audit fingerprints omit these exact body columns only after verifying their live bytes or the independent external purge receipt; an inline-only restore still fails verification. Their metadata deadline is 90 days; this module does not claim that SQL deletion vacuums the database file.

The code derives deadlines from database time and persisted lifecycle rows. Ordinary retired derived bodies wait one hour; explicitly failed/cancelled Nextcloud builds wait 24 hours after their fence retirement, and pending subtasks or valid read/build leases still protect them. Confirmed source tombstones and recorded entire binding withdrawal skip the ordinary body window after access/build closure. A current owner, another shared owner, or a temporarily unavailable source retains the body. Batches rotate by the database clock timestamp of the last actual check, so protected first rows cannot indefinitely starve later due bodies. Original file recovery objects continue to use the existing resource GC protocol, with at most the prior recovery version and the seven day maximum; the body vault does not store file bytes as a second recovery file.

`OriginalBodyRetentionStore.PurgeSelectedJob(ctx, tenant, jobID, limit)` selects one persisted GC job and only its exact body ownership, original parent tuples, and datasource name inputs. Every destructive body transaction repeats the current web administrator check when invoked by `CollectJob`, plus source, scoped coverage, read/build leases, shared current ownership, DB clock, and original field identity checks. `RunDue` invokes the same selected job phase. `original_body_gc_items` tracks body progress independently of the existing resource kind constraints. The preview category `sql_original_body` and status aggregates include these rows. Its items remain pending/blocked while bodies are retained or unverifiable. Their confirmed physical released bytes are zero: SQL byte removal is not disk release. UI job state alone is not evidence that all body copies were purged.

## External anchor bootstrap

The runtime refuses migration 152/71 startup until the external journal matches the database. A fresh initialization assigns a random journal UUID to the database singleton and to the HMAC signed external pin. Sequence zero without this identity is insufficient. The journal UUID, authenticated hash chain, purge epoch, body hash, and scope hash prevent a restored database or inline body from silently reviving a purged original.

The app and offline CLI must use the same effective Unix UID. Required paths are absolute, canonical paths with no symlink parents. Journal directory mode is `0700`; key, ledger, and pin are regular files of mode `0600`, owned by that UID and with a single hard link. Key bytes must be at least 32 bytes. The pin and key must remain outside the database restore/rollback scope. Provision an independently retained pin location; do not restore the ledger, pin, and key to an old generation together with the database. A reused/conflicting directory or existing pin is not adopted.

Required environment variables:

```sh
export WEKNORA_ORIGINAL_BODY_JOURNAL_DIRECTORY=/var/lib/weknora-body-ledger
export WEKNORA_ORIGINAL_BODY_JOURNAL_KEY=/var/lib/weknora-body-key/hmac.key
export WEKNORA_ORIGINAL_BODY_JOURNAL_PIN=/var/lib/weknora-body-pin/pin.json
export WEKNORA_ORIGINAL_BODY_MAINTENANCE_DSN='postgres://USER:PASSWORD@HOST:5432/DATABASE?sslmode=disable'
```

For PostgreSQL the maintenance DSN is a PostgreSQL URL, usable by the existing migrator and Gorm. Use the same embedding migration options as the app; stock PostgreSQL without vector/search extensions requires the existing `options=-c%20app.skip_embedding=true` setting. SQLite instead uses an absolute raw database file path as the maintenance DSN, and `-driver sqlite`. The CLI runs from `/app`, where the normal image includes the repository migrations.

Fresh installation, with app readers, builders, and synchronization stopped:

1. Run `original-body-retention -driver postgres -mode migrate`. This invokes the actual existing database migrator, does not recover dirty migrations automatically, and creates migration 152 through the complete migration chain. The normal `docker/Dockerfile.app` builds and installs this executable at `/usr/local/bin/original-body-retention`.
2. Provision the three external directories for the runtime UID with mode `0700`, generate a unique random key with mode `0600`, and leave ledger and pin absent. For example, under that UID: `umask 077; openssl rand 48 > "$WEKNORA_ORIGINAL_BODY_JOURNAL_KEY"`.
3. Run `original-body-retention -driver postgres -mode init`. It refuses an existing anchor, a previously initialized DB UUID, nonzero DB journal state, purge receipts, or pending/purged bodies. It creates the external anchor and binds its fresh UUID to the DB in the initialization transaction.
4. Run `original-body-retention -driver postgres -mode verify`, then start the app with the same three external path variables.

Development equivalent: `go run ./cmd/original-body-retention -driver sqlite -mode migrate`, then `init`, then `verify`, from the repository root, with the same environment. Production uses the packaged executable, not the Go toolchain.

## Existing 152/71 schema forward step

Fresh migration 152/71 includes immutable `original_body_lease_scopes`. Actual authenticated message producers record resolved tenant/KB scope rows in the body capture transaction; an empty KB row records completeness, and a lone empty KB row records a complete no-KB original. The original scope rows enter the external purge scope hash. KB collection protection follows these captured rows after a current knowledge pointer moves. Actual message readers also claim exact body ID leases before hydration, including no-KB history, renew them with the material guard, and release them when that guard closes.

For an earlier installed 152/71 schema, stop the app/readers/builders/sync, retain the database and external anchor, run `original-body-retention -driver postgres -mode upgrade-scopes`, then `reconcile`, then `verify`, and reopen the app. Use `-driver sqlite` for SQLite. This explicit offline step creates the scope table and its immutable update/delete triggers; it never inserts scope rows from current pointers, reinitializes the journal, or grants content lease coverage. Old bodies with no original scope inventory, or partial KB rows lacking that completeness marker, remain retained. Startup verification refuses a missing scope table, required column, or immutable scope trigger even with a matching sequence-zero pin. Do not downgrade to a version that omits this check.

## Restore sequence

Stop app readers/builders/sync. Restore the database while retaining the current external anchor. Run `original-body-retention -driver postgres -mode reconcile`, then `original-body-retention -driver postgres -mode verify`, then reopen the app. Reconciliation validates every event and clears only the exact restored body/inline leaf. If the restored DB has a current owner, active lease, changed identity, or unexpired window, reconciliation refuses; independently reestablish source closure before retrying. Never initialize a new empty anchor to make a restored DB start.

A DB ACK failure after durable external append causes reads to fail closed until replay. An authenticated ledger tail with an older valid pin is the interrupted pin-write boundary and can advance that pin; truncation below a pinned head, a forged chain, a different UUID, wrong key, missing file, or permissive permissions is rejected. If initialization wrote the external files but failed to commit its DB UUID, preserve both sides for explicit recovery; a second `init` does not adopt those files.

Neither migration, `init`, `reconcile`, nor runtime startup inserts content lease coverage. Selected job GC needs the existing exact knowledge/source scope inventory and coverage evidence. Unknown historic, external Neo4j/search, Wiki, backup, or unclassified SQL copies remain independent blockers; absence of current rows is not a complete inventory or deletion ACK. The 150/69 upload input stores artifact/control identities rather than raw file bytes. New auto-tag tenant controls omit credentials from serialized snapshots; older already stored sensitive snapshots require separately scoped inventory and purge.

Old generated-field or fork `parent_materials` snapshots with nonempty `Messages` arrays are recognized as unsupported original-body copies by inventory/capture. Their proof metadata stays inline and is not rewritten or re-signed. Without a durable original copy/closure receipt they remain explicit retained blockers after message closure; this release does not claim their removal. New generated-field and fork writers persist metadata-only parents.

Acceptance evidence records the actual tested source and module limits in `docs/evidence/original-body-retention-20261009.json`.
