# Native original frames for running Continue

Parent: `cee02ace1798324eb0659b1f72ec6c9910d25cef`. No new SQL migration.

The actual message service now obtains control metadata and original generation identity through the production transaction-bound current owner/actor factory. `GetMessageForControl` reads metadata only; stop may control an old unavailable message without reading its body. Native replay requires immutable generation/admission/version continuity. `GetMessageForStream` no longer loads a full message body for native running material.

The actual `setupStreamHandler` wraps its StreamManager. Every generated frame is admitted from the actual producer arguments, complete original source lineage and original parent proof/control snapshots. Parents contain metadata only (`Messages=nil`). Repeated event IDs are distinguished by a private monotonic frame sequence. Redis append happens only after the exact producer checks; cached Redis text cannot create or replace a receipt.

Running Continue uses a private registry keyed by actual service, owner tenant, session owner, session, request, message and generation. It first reads frame metadata, acquires fresh receiver original parent/source KB and exact Nextcloud leases, verifies the native sequence/digest/TTL again, then copies and decodes the original frame bytes. Each Write and Flush independently checks receiver identity, all original parent proofs, current source authorization, actual lease validity and the native frame identity. Source/actor permission is never borrowed from the producer's context.

Frames expire after two minutes and the registry caps each run at 8 MiB. Expiry or completion removes body bytes. Completed Continue switches to the existing original saved-message reconstruction. The initial raw query is omitted from running replay. Fixed code terminal errors use a static recipe; backend error text is never admitted. Missing registry, restart, another application instance, unknown legacy frame or mutation is unavailable; new legal same-instance running turns remain usable.

## Actual acceptance

SQLite pool1 with deadlines: real paused KnowledgeQA producer → first HTTP disconnect while producer retains leases → second running Continue → original native frames; forged same-source Redis frame text is ignored and the receiver reads zero Redis events. Actual completion then uses the original saved replay. Body ABA and current actor/source revocation at public URL rewriting block output; unknown cached bodies without registry are unavailable. Metadata lookup projections omit original bodies. Fresh PostgreSQL pool1 verifies original producer → receiver scopes and current caller/body ABA.

Metadata peek/body-copy expiry, body mutation, source metadata mutation and closed registry are rejected. Previously implemented live HTTP and completed replay positives/revokes continue to run.

## Remaining work

Native running replay is limited to the application instance that produced its immutable frame receipt. Distributed native frame persistence is not implemented. Root independently integrates newly retrieved RAG search/hydration physical leases through model/output. Rewind, steer injection/control receipts, memory direct cursor extraction, artifact metadata/body reads, message search/pair reconstruction and actual cross-tenant Agent acceptance remain active. Repository-wide caller inspection found no standalone session/message transcript import/export API; this module does not add one. Unknown prior materials are never certified from current bodies or metadata.
