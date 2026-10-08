# Durable capacity for local original files

Migration PostgreSQL148 / SQLite67 adds a durable relationship between a new
capacity reservation, its exact future knowledge ID, backend, filesystem/root
identity, unique physical path and the resource catalog row. Old reservations
retain protocol version zero; absence of an intent does not prove that their
historical upload never started.

The actual Nextcloud ingestion path installs a server write session before
retry admission, file save, catalog registration and staging. New reservations
are allowed only for providers that implement the local intent protocol. Unknown,
remote bucket and unsupported platform providers remain unavailable without
writing. The actual selected file provider is checked again before SaveFile.

A local write takes an exclusive cross-process flock and persists `prepared`
before creating the object. Every path component uses directory FDs with
NOFOLLOW; the lock directory/file must be private, owned and unlinked. The file
name contains the reservation UUID and creation is exclusive. Only a successful
actual create records its device/inode identity and transitions to `writing`.
Bytes cannot be written before that durable receipt. An existing legacy object
at the planned name is never adopted. A crash between create and its receipt
retains the unknown charge for owner review.

Each write chunk and catalog boundary rechecks the actual reservation, writer,
scope and lease. Successful local writes fsync the file and directory entries,
record actual bytes/hash, and bind the actual catalog resource. The OS fence
remains held through catalog registration, knowledge creation, source staging
and Finish. It is released only when the ingestion invocation exits. Closed,
expired or reconciled workers cannot resume writes/catalog operations.

Every normal failure invokes conservative cleanup. A new-protocol reservation
with no pre-IO intent can be cancelled under the reservation lock. A failure
that entered the physical path retains the object. Catalog/knowledge failure
cleanup skips physical deletion for this session. Reconciliation obtains the
same exclusive provider fence, re-reads the DB state and KB/backend binding,
observes the actual exact file/inode/hash, and locks/rechecks its actual catalog
resources and owner bindings. A current writer remains a refusal even if its
reservation lease expired.

An actually absent file with no catalog owner may release its reservation.
An existing retained original is charged to its real resource/KB. An actual
partial or unregistered object becomes a committed orphan charge for its
observed bytes, retained against the original KB. This orphan amount is included
in logical retained bytes, retention quota and capacity status. A fresh provider
measurement supersedes the temporary disk reservation buffer. Reconciliation
does not delete a file or resource, remove an owner, create a GC claim, or
authorize publication. Shared ownership, mismatched backend/KB/root, linked
paths and changed bytes/inodes are handled conservatively.

The persisted orphan row retains the exact object location and identity for a
future coordinated GC inventory. That future deletion and charge retirement
workflow is not implemented here. Until it exists, retained orphan bytes remain
charged. Full local original files still count through the actual catalog.

After a crashed process, use the local DB-owner maintenance command with the
same configured storage base, tenant/backend registry and schema:

```sh
capacity-reconcile --mode=reconcile \
  --postgres-connection-file=/absolute/private/database.dsn \
  --reservation-token=<exact-original-token> \
  --local-storage-base-dir=/absolute/original/local-storage-base \
  --output=/absolute/private/new-reconciliation-receipt.json
```

`--sqlite-db` accepts an owned mode0600 DB in a mode0700 directory. DSN inputs
and outputs use the same owner/private restrictions; outputs are reserved with
O_EXCL before any DB mutation. `--mode=inspect` returns current persisted evidence
for the exact token. Outputs include internal object paths and must stay private.
No ordinary HTTP endpoint, actor grant or deletion option is introduced.

Run the bounded owned verification fixture:

```sh
python3 scripts/ops/isolated-capacity-write-intents.py
```

It uses stock PG16 on a nonce internal network with no host ports, 1CPU/512MiB
tmpfs, and a 1CPU/2GiB Go1.26 runner. Tests execute the production Nextcloud
ingestion entry point, actual local SaveFile and resource catalog, and actual
owner CLI on SQLite and PG after subprocess `os.Exit(77)` without normal cleanup.
Queue/parser and source-version outcomes are controlled fixture adapters.
Before-IO failure, real partial bytes, register/stage failure, duplicate retry,
two competing actual writers, expired active writer, late writer/catalog,
shared owners, linked paths, cross root/backend/KB and legacy unknown cases are
covered. Fixture schemas, files and Docker resources are removed afterwards;
existing Go caches and shared application resources are preserved.

This covers local original source-file admission and failed/crashed write
accounting. Derived assets, index/backend writes and bucket capacity require
their own durable intents. The existing actual RunDue/physical GC path still
needs complete reader/build coverage and content-fence integration; this module
does not infer that protection from a separate dormant GC helper.
