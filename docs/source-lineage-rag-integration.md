# Ordinary RAG lineage integration (private Phase 3)

Status: implementation in progress; no rollout or PRD completion claim. This
private source tree wires accounted ordinary text inputs, typed message reads,
atomic generated-field updates and durable stream-prefix proofs. The trusted
database hydration API and production KB/merge adapter are wired by DI. The
adapter captures the actual database chunk/metadata and original durable lease
epoch in one snapshot, before progress or model input. Source reads without
this capability remain unknown and denied. Session title storage/legacy title
reads remain a separate required delta.

## Contracts and actual boundaries

- `RAGSourceLineage.CheckedSnapshot` returns the exact envelope checked against
  current grants. Concurrent additions require another check; unknown and
  bounded-set overflow cannot become complete again.
- Ordinary session entry always creates the accumulator with the current
  mandatory guard and original selected Agent/KB scope. Effective retrieval
  tenant does not replace the captured caller/principal.
- Retained assistant prose inherits the original saved envelope independent of
  citations. Query-understanding independently loaded history is also checked.
  Every rewrite, rerank and final chat call checks its actual inputs; the final
  model messages carry private metadata excluded from provider JSON.
  Their wire-visible nested fields and options are detached before the final
  authorization callback, so aliases cannot change the authorized payload.
- Images, captions, attachments, quotes, memories, graph/entity analysis, web,
  data analysis and document-listing model fallback have no complete adapter in
  this delta. Their actual use stops the operation before the model or generated
  source output. No-search intent does not discard previous dependencies.
- Message/checkpoint reads require complete saved proof; NULL is unknown.
  Raw user Content survives while derived user fields are stripped. Agent raw
  repository history requires the same mandatory per-row/checkpoint verifier.
- Prefix events persist their private envelope in the stream store. The public
  StreamResponse never exposes it. The handler unions every producer receipt,
  rejects missing/unknown receipts and cannot shrink prior sources. Reconnect
  checks each prefix, its current authorization and final-message consistency.
  The private running-message lookup is owner scoped; it does not expose its raw
  content before prefix checks.
- Repository updates explicitly persist empty body/reference/step values and
  NULL lineage inside the answer/artifact transaction. Completion is announced
  after the message write. An independently persisted checkpoint and its own
  envelope are preserved rather than overwritten by a stale final answer.
- Unknown/controlled answers cannot enter history indexing, memory copying or
  suggestions until those derivative adapters preserve and check provenance.
  Existing ordinary passages have no reliable producer origin; current channel
  or missing retained Nextcloud evidence is not a source-free proof.

## Compatibility and remaining work

Legacy and uncovered Agent generated history/output is closed instead of using
the earlier ever-source heuristic as a complete-proof exemption. The independent
Agent engine/tool/compaction producer delta must supply actual receipts before
those paths regain access. Dynamic skills/tool definitions must bind their own
actual input values; unrelated static prompt receipts are insufficient.

The new source-independent title generator checks only raw user Content and a
static configured title prompt, but durable title provenance and every legacy
session-title consumer still require the separate title delta. Resource/artifact
HTTP paths call the mandatory message reader before delivering generated fields;
artifact/image producer coverage remains closed. Ordinary passage REST previews,
exports and derivative inventories require their own provenance gate. Source
lineage does not establish full GC read-lease coverage or real AD acceptance.

## Focused evidence

`TestRAGSourceLineageActualEntryKeepsCitationFreeNoSearchDependency` runs the real
service event loop, renderer, history loader, rewrite model and final stream. A
trusted reader fixture supplies the first source receipt; later citation-free
follow-up/no-search intent inherits it and revocation blocks it before a model.
Fresh accounted plain Q&A remains complete-empty. Database hydration races are
tested at the real SQLite/PostgreSQL snapshot/lease reader, with committed
withdrawal between document reads and current fence/lease denial afterwards.

Additional actual-boundary tests cover opaque inputs, legacy/checkpoint/message
reads, raw user redaction, event adapter privacy, durable prefix roundtrips and
replay denial, explicit NULL/zero saves and a failure after the final generated
field update proving full transaction rollback. Tests use cached Go with one
CPU; no full images or shared runtime are changed.
