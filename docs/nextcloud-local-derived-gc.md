# Actual local derived inventory and GC

## Protocol and accepted runtime

Migration PG149/SQLite68 establishes a new origin boundary. Only a Nextcloud knowledge inserted after this boundary and a new original-input admission may receive a backend plan. Existing knowledge, configuration scans, empty row counts and legacy rows cannot create this evidence. The actual Composite engine selection persists the sorted backend set before physical derived writes. Graph, Wiki, bound stores, remote engines and separate derived databases remain observable `derived_backend_inventory_unverified` blockers.

The actual chunk repository and PostgreSQL/SQLite index transactions record immutable exact physical IDs and dimensions with the selected plan. The retirement seal checks every observed local row against these writer facts and retains the union of written IDs, including rows removed by a covered retry. Native SQLite FTS5/vec0 remnants are therefore inspected and deleted even after metadata was removed. The manifest contains identities and hashes; it does not duplicate document bodies.

`NextcloudGCStore.RunDue` performs local SQL deletion with KB row → content KB fence → source/job/generation locks. Existing complete/drained coverage, broad and exact active read/build leases, current source candidate, knowledge status and bounded claim CAS remain required. No production coverage marker is inserted. SQLite metadata, FTS5, native vec0 and their durable exact deletion receipts share one transaction. PostgreSQL uses the actual pgvector adapter and exact relational row deletion. Receipt failures roll back data deletion. A collected local manifest is periodically verified; source-current or row restoration is reported and never authorizes a second deletion.

The resource provider phase retains its actual guarded unlink/ACK. Manifest counters distinguish physically collected objects from protected references still owned by another entity. SQL receipts report actual logical/native row counts and do not claim PostgreSQL or SQLite allocated pages were reclaimed. Confirmed released filesystem bytes come only from the provider's successful unlink ACK.

## Local pathname fencing

All local application writers/deletes take an exact path flock, including legacy timestamp paths. Writers create with O_EXCL through NOFOLLOW descriptor walks. Unix GC atomically moves a pinned inode into an owned private holding directory with a durable path/dev/inode receipt before unlink. A same-path replacement is restored without overwriting or kept for review; it is never unlinked as the original. A real child process exit after receipt+rename is recovered from this persisted inode. Unsupported platforms refuse this protocol.

## Scope of reader/writer audit

| Entry | Relevant base caller/fence | Acceptance boundary |
|---|---|---|
| RAG/Agent/search/REST | knowledgebase search broad lease, exact hydration guard; knowledge graph lineage snapshots retain expanded leases | Existing current source/ACL and original-input checks are independent of the GC lease |
| background summary/question/image | original-input capture + private Nextcloud build lease; chunk and index TX validation | Actual model/Composite/local index pipeline is exercised here |
| chunk/index mutators/copy | SQLite transactional guard and surfaced FTS/vec errors; PG copy uses guarded writer | Protected direct calls need their current worker; missing proof fails |
| FAQ/enrichment/span/tag manifests and parents | exact retirement inventory hashes existing admissions/material/actor/config/history/span/classifier/FAQ rows | This module targets exact Nextcloud local physical rows; it does not adopt legacy or external descendant output |
| graph/Wiki/external vector | planned use remains an explicit blocker; no prefix or namespace deletion | Full external writer receipts and deletion ACKs remain required |

The frozen base predates root's later all-entry lease module; this delta does not claim global read/build coverage or enable ingress. Wiki direct page/index/graph reads and remote backend lifecycle must be assessed with that module before production coverage is activated.

## Audit retention remains a separate lifecycle

Existing immutable history/enrichment/span/classifier/FAQ tables can contain fulltext in JSON. Their row digests and byte sizes are inventoried separately. The existing `AuditRetainUntilMS` field records the proposed 90-day metadata/audit deadline, not permission to retain fulltext until that date. PRD §7.4 permits 90 days only for metadata without bodies; retired derived bodies have a one-hour safety window, terminal failed build bodies 24 hours, and confirmed deletion/whole-binding withdrawal requires prompt cleanup. Current/shared material and temporary source unavailability have different preservation rules. Physical `collected` is not a claim that those bodies were purged. A body/metadata separation and exact expiry purge module is still required; neither the manifest nor this module labels these bodies permanent audit metadata.

## Reproduce

Run `scripts/ops/isolated-nextcloud-derived-gc.py --evidence-dir /absolute/new/private/directory` from clean frozen source, then run with `--native-vector` and a second new directory. Source mounts are read-only; output is exclusive and nonce-owned; TCP PostgreSQL readiness avoids temporary initialization server false positives. The script records actual Git HEAD, immutable images, resource limits and logs, verifies fixture schemas are removed, removes only exact nonce resources, and preserves shared Go caches. Stock PG validates the actual adapter's storage/GC path; the native run additionally executes real halfvec vector retrieval. SQLite uses sqlite_vec.Auto() and native FTS5/vec0.

## Frozen actual evidence

Runtime/source: `76d82e1f6745c00eca60ff78d8d0fb78f651a047`; parent: `997462a45abda81cb42cb778f5a83e3ebf9c3818`. Stock probe: SQLite 12 modes and PostgreSQL 10 modes PASS, including real owned provider/catalog original unlink and confirmed bytes, active read/build leases, late writer denial, current/foreign parent restoration, SQL/native receipt rollback, untracked-row blocker and native orphan cleanup. Native probe: real public.halfvec adapter retrieval and RunDue 10 modes PASS.

Both read-only probes left source clean and unchanged, owned schemas zero, exact fixture resources removed and caches preserved. Fixed image digests and log SHA256 values are in the adjacent evidence JSON. The frozen original object-GC tree remains clean.

Development failures were retained in private nonce directories: a compile call used the wrong repository method (fixed), an original fixture omitted its ownership binding (GC refused), and the PG original fixture omitted the indexed-withdrawal table (42P01 before deletion, fixed). Their own resources were also cleaned. These failed probes are not acceptance evidence.
