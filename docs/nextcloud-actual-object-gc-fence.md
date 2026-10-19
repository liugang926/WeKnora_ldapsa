# Content leases on the actual local-object GC path

`RunDue` now gates its actual source-file and extracted-image provider phase on
the existing content fence and lease registry. This does not insert a production
coverage marker or enable deletion for an unverified reader/build rollout.

Physical claims take the KB row before the content KB sentinel, following the
source-version writers' order, then re-read the job/version/knowledge/item and
resource. They require an existing retired exact generation with matching
datasource/external identity, its durable retirement boundary, a complete and
drained reader/build coverage record, and no live broad KB or exact read/build
lease. Missing or unavailable proof is an observable blocked reason. Actual DB
time gates retention and bounded claim expiry; passing a future scheduler time
does not expire a durable claim or shorten original-file retention.

When the final owner is released, the object item and exact content generation
enter `deleting` with the same bounded UUID claim, and the exact content epoch
advances. The existing `AcquireKBRead` active-GC check blocks late broad reads.
Existing exact retirement also rejects new exact reads and builders. Another
object phase of that generation must wait for this bounded claim.

Database-backed provider configuration is prepared outside the final GC
transaction. The returned operation performs physical IO only, so the SQLite
single-connection pool can run this path without reentrant DB access. Before
calling the provider, the final transaction locks the KB/content scope and
rechecks the exact token/epoch/expiry, coverage, active leases, original/source
deadline, current version, knowledge state and resource owners. Those locks stay
held through the actual unlink and DB acknowledgement; source restoration and
new readers cannot enter between admission and physical release.

The GC-specific local provider uses directory FDs and NOFOLLOW for every path
component. It refuses symlinks, nonregular objects and hard links, checks the
opened object identity, unlinks the exact entry and fsyncs its parent directory.
Capacity-protocol objects also take their original local writer flock. The
ordinary file deletion API is unchanged. Remote/legacy unscoped providers have
no physical deletion authority from this path.

One object acknowledgement returns the exact generation to `retired`, clears
only the matching token and never declares the whole generation `deleted`.
Unknown derived/index/graph/Wiki inventory still prevents complete job
collection. Shared resources keep their other owners and release zero physical
bytes. Failed provider calls preserve the deleting resource and clear only this
claim by token/epoch CAS. A stale worker cannot clear a successor claim. A crash
after unlink but before receipt can finish an absent object idempotently with
zero new credited bytes. An expired claim must recheck any newly admitted broad
or exact lease before retrying.

The owned probe is reproducible against a clean frozen checkout:

```sh
python3 scripts/ops/isolated-nextcloud-physical-gc.py \
  --evidence-dir=/absolute/new/private/gc-probe-<nonce>
```

It requires a local Unix Docker context, starts only nonce-labelled stock PG16
on an internal network with no host ports, waits for TCP readiness, and mounts
the source read only into the Go1.26 runner. It records the actual Git HEAD and
checks that source/HEAD remain clean and unchanged. Evidence goes to the new
operator directory, never over an existing frozen report. Limits are 1CPU/512MiB
tmpfs PostgreSQL and 1CPU/2GiB Go. Existing module/build caches are preserved.

The fixture markers are explicitly synthetic complete/drained proof for the
controlled test readers and builders. SQLite and PG tests call actual RunDue
and the secure local provider for coverage absence, real broad/exact reader and
builder leases, late reads, shared owners, provider failure, restored current
source, successor CAS and symlink refusal. Real subprocesses exit after claim
commit or actual unlink without normal cleanup. An expired crash claim cannot
delete past a newly admitted broad reader; after release the real provider
collects the file and preserves zero credit for an unacknowledged earlier
unlink. PostgreSQL additionally proves that the actual provider transaction
blocks a concurrent restoration update until unlink/receipt finish. Every
fixture schema/file/container/network is removed afterwards.

Production coverage remains absent unless a separate rollout establishes every
reader/build path and drains legacy work. This implementation closes the local
object deletion path's content-lease gap. It does not establish all derived/index
backend deletion or complete shared-stack recovery/ingress acceptance.
