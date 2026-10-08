# Publication recovery module evidence — 2026-10-09

Base: `87e83cea7ca8c8f724377920c61d3be8636e3e21` (C6 output-lineage integration).
The base checkout was read only; implementation used a separate detached worktree.

Runtime pins:

- Go 1.26 test image: `weknora-go-test@sha256:5a74e8b5137496d3e22c72c01d862c4cbb1fe6ce476ddf5616fd9a807d6ae9ac`.
- PostgreSQL 16: `postgres@sha256:20edbde7749f822887a1a022ad526fde0a47d6b2be9a8364433605cf65099416`.
- Go runner limited to 1 CPU / 2 GiB, module cache mounted read only and existing build cache reused.
- Owned PostgreSQL container limited to 1 CPU / 512 MiB, tmpfs data directory, an owned internal network and no host ports. Exact container ID was `da2d6786b7d2a331a8ec6d340dd2ffbbbeaa4aa45660475b67f19266f777fcef`.

Focused Go verification passed:

```sh
go test -p 1 ./cmd/publication-recovery ./internal/database ./internal/application/repository \
  -run 'Test(RecoveryPrivate|NextcloudPublicationRecovery|PostgresNextcloudPublicationRecovery|NextcloudRecovery|MigrationDirectoriesLoad|SQLite)' -count=1
```

This included private owner-file restrictions, exclusive output receipts,
SQLite fresh/upgrade migration checks through 64, real SQLite `VACUUM INTO`
backup/rollback/replay, PostgreSQL migration 145 and replay, all original
bindings including an untouched binding in another tenant, incomplete inventories,
changed source tuples, out-of-order/gapped/duplicate/null signed fields, source
history gaps, old build-lease/sync-worker closure, immutable scope receipts,
retained registered imported-file bytes and denied unknown GC claims.

Public closure checks called the actual `NextcloudPublicationGuard`, RAG source
lineage resolver and saved history lineage recheck. Reconciliation called the
actual signed source-pairing commit acknowledgement client, the full double
manifest probe, and the signed publication-check client for a newly ingested
knowledge ID. Wrong remote pair/instance, changed generations, withdrawn-file
reappearance, a stopped binding and an old publication epoch were denied.

The PostgreSQL rollback checks additionally passed inside the owned database
container with the actual `pg_dump` and `pg_restore` binaries:

```sh
WEKNORA_RECOVERY_PG_DUMP_RESTORE=1 \
WEKNORA_LEASE_TEST_POSTGRES_DSN='host=127.0.0.1 user=recovery dbname=recovery sslmode=disable' \
  /tmp/recovery-fixture.test -test.run '^TestPostgresNextcloudPublicationRecovery' -test.v
```

Both `TestPostgresNextcloudPublicationRecoveryClosure` and
`TestPostgresNextcloudPublicationRecoveryNewGenerationRequiresLiveStableManifest`
passed after restoring backup-era ready source versions. The runner was built
with static linkage for the Alpine PostgreSQL test container; required public
Jieba dictionary files were copied into that owned container only. No external
account, paid model API or shared service data was used.

Per-test nonce schemas were removed (verified count 0). The dedicated container,
internal network, static fixture binary and copied dictionary staging files were
removed after verification. Existing deployment containers and cache volumes
were preserved. `git diff --check` passed.

These results establish the module's closure/reconciliation contract. Complete
shared-stack recovery acceptance still requires the root orchestration to bind
the source-inventory digest into the coordinated checkpoint, verify both replay
receipts, perform authoritative directory/ACL and physical/index inventory
reconciliation, and gate ingress reopening. This module does not declare that
larger acceptance completed.
