# WeKnora publication recovery closure

This local maintenance command consumes the external Nextcloud journal's signed
schema 1 plan. It closes restored publication before ordinary readers and workers
restart. It exposes no HTTP route, creates no personal principal and uses no
machine credential as a personal grant.

## Backup evidence

Stop WeKnora ingress and all writers before capture. Keep the inventory in a
private, owner-only directory outside both restored volumes and checkpoint
inputs. Use the independently retained Nextcloud terminal journal record and its
instance, stream, sequence and database prefix from the checkpoint anchor:

```sh
publication-recovery --mode=capture \
  --postgres-connection-file=/absolute/private-operator/database.dsn \
  --instance-id=<original-instance> --stream-id=<original-stream-uuid> \
  --checkpoint-record-sha256=<external-checkpoint-record-pin> \
  --checkpoint-sequence=<external-checkpoint-sequence> \
  --checkpoint-database-chain-sha256=<external-checkpoint-prefix> \
  --output=/absolute/private-external/weknora-inventory.json
```

The output contains the exact persisted operation, tenant, KB, datasource,
instance, binding, origin, encrypted configuration fingerprint, publication
epoch, key identifier and pairing state for every restored pair of that
instance. Its source-state digest covers every current source version, retained
knowledge identity/metadata, contiguous append-only source history and content fence in those dedicated KBs. An unpaired
source, missing candidate or ambiguous original tuple prevents capture.

Retain the printed SHA-256 independently and bind it into the coordinated
checkpoint manifest before copying the database. Capture is evidence from real
rows; manually supplying a body or an `assert` flag cannot replace it. The
coordinated checkpoint's own digest and external journal pins remain required.

## Apply the signed plan

Restore and verify the coordinated checkpoint with both stacks' ingress and
ordinary workers stopped. Run migrations, keep Nextcloud in maintenance and
apply its matching closed replay receipt. Then run WeKnora closure as the
configuration/database owner:

```sh
publication-recovery --mode=apply \
  --postgres-connection-file=/absolute/private-operator/database.dsn \
  --plan=/absolute/private-operator/external-plan.json \
  --key-file=/absolute/private-operator/journal.key \
  --inventory=/absolute/private-external/weknora-inventory.json \
  --inventory-sha256=<independently-retained-inventory-pin> \
  --output=/absolute/private-operator/weknora-closure-receipt.json
```

The journal key must contain exactly 32 bytes. Inputs are regular files with
mode 0600 under an owned mode 0700 directory; symlink paths and hard links are
refused. PostgreSQL requires the database owner role. SQLite uses an owned mode
0600 database under a private directory via `--sqlite-db=/absolute/path/db`.
Outputs use exclusive creation and fsync. Supply the configured AES environment
and approved Nextcloud origins through the existing private operator transport.
Credentials and configuration bodies are never printed.

The command validates the HMAC of the exact Python envelope's `plan` string,
rejects duplicate/unknown JSON fields, validates a contiguous database-prefix
range, and matches the full pinned restored inventory. It closes **all** restored
pairs for that instance, even if a binding had no event after the checkpoint.
An interval binding without a restored pair is considered absent only after the
complete local registry and pinned inventory match; it opens no scope.

In one transaction it clears every retained publication ETag, tombstones source
versions and post-checkpoint withdrawn/deleted files, retires the KB and all old
knowledge epochs, pauses sources, cancels running sync logs, invalidates pending
retry claims and revokes restored GC claims. Physical rows, chunks and files stay
intact. Restored content-lease coverage is removed so unknown physical/index
inventory cannot become deletion authority.

PostgreSQL migration 145 and SQLite migration 64 persist the plan receipt and
original-pair scope digests. The scope also records every actually retired knowledge ID/epoch and the closed
KB epoch. An identical retry rechecks those monotone fences and actual closed
source/worker state before returning the same receipt. A partial restore that
retains a receipt while reviving an old generation is refused. A
changed plan for the same checkpoint, changed tuple or inventory, incomplete
range or reversed recovery boundary fails closed. Admission triggers reject
ordinary source resume before reconciliation and reject staging/publication of
retired immutable knowledge IDs. Safety receipt migrations refuse rollback.

## Permit new generations

The cross-service recovery procedure must separately reconcile current directory
identities, ACLs, physical/index manifests, Nextcloud's closure receipt and the
coordinated checkpoint. Keep public ingress closed throughout. Only after the
original Nextcloud binding is deliberately resumed, probe that exact paired
source through the local maintenance command:

```sh
publication-recovery --mode=reconcile \
  --postgres-connection-file=/absolute/private-operator/database.dsn \
  --plan-sha256=<persisted-closure-plan-digest> \
  --pair-operation-id=<original-pair-operation-id> \
  --output=/absolute/private-operator/weknora-reconciliation-receipt.json
```

This command reads the persisted machine configuration itself and verifies the
original remote operation/tenant/KB/datasource pairing with its signed idempotent
commit acknowledgement before and after scanning. It checks the
live instance, binding, origin and an active publication epoch newer than the
backup's epoch. It performs two complete manifest scans with the same generation
and entries, enclosed by live identity checks. A partial page, changed generation,
wrong source, ACL error, or reappearance of a replayed withdrawal fails closed.
The persisted reconciliation body and digest bind the full observed manifest
to the plan and original source pair; the private output contains the same
reviewable observation. A pending, uncommitted pairing remains closed for the
normal pairing repair procedure.

Successful reconciliation advances and reopens only the containing KB fence,
resumes the paired source and clears its stale sync cursor. **Old exact knowledge
fences remain retired and their source versions remain tombstoned.** A subsequent
full ingestion must create new immutable knowledge IDs, obtain new build leases
and pass the ordinary signed publication check. Current personal SourceGuard,
RAG/Agent/history lineage and LDAP/ACL checks continue to apply. Reconciliation
does not create global ingress permission or reinstate unknown GC coverage.

## Verification

The focused contracts exercise a real SQLite `VACUUM INTO` checkpoint followed by
post-checkpoint tombstone, old-DB restore and replay. They check actual public
publication guard denial, RAG lineage resolution denial, saved history lineage
recheck denial, cancellation of a restored worker/build lease, identical retry,
changed plan and cross-instance/pair rejection, retained file bytes, stable live
machine manifest reconciliation and successful publication of a newly ingested
generation through the actual signed publication-check client.

PostgreSQL tests use a separate nonce schema in an owned PostgreSQL 16 tmpfs
container on an internal network with no host ports. The explicit
`WEKNORA_RECOVERY_PG_DUMP_RESTORE=1` fixture runs `pg_dump` and `pg_restore` inside
that container to restore the old ready source versions before replay. This is
an isolated module contract, not acceptance of a complete deployed shared-stack
recovery. The combined recovery entry point must require both receipts and its
remaining authoritative checks before reopening ingress.
