# Agent actual-input producers and current limitations

Source only; this does not enable rollout, change the deployed runtime, close
V1, authorize derived indexing/GC, or provide a complete adapter inventory proof
for the product. The implementation uses the frozen Phase 2 c6 source plus the
shared private lineage contracts and Phase 3 actual chunk snapshot reader.

The engine accounts its actual `chat.Message` inputs, model control arguments,
reasoning/provider artifacts, static function schemas, every trusted tool
result, and source output. A response, final synthesis, fallback and stored
answer inherit the conservative whole-turn union. KB tool-body redaction,
same-role message merging and context truncation cannot clear a dependency.
Nullable/legacy input envelopes stay unknown. Model citations and client/model
JSON cannot set private receipts.

The mandatory `SetSourceLineageVerifier` callback is bound by the service to the
original interactive caller, the effective source tenant and current session /
Agent scope. Every model-send, model chunk, image describer, compactor input /
output, event, checkpoint and final completion checks it. An error latches and
cancels the run. Rejected prefixes cannot publish source names in diagnostic or
control events. All engine events carry a typed private envelope; event
publication is serialized and uses `CheckedSnapshot` so concurrent tool reads
cannot publish an envelope different from the one authorized. The service and
stream handler still own atomic final persistence and durable prefix sequence /
turn binding; this producer code does not establish those gates itself.

The actual `read_document` producer uses `RAGChunkSnapshotReader`, the original
`NextcloudReadGuard.ExactLease` token and the metadata receipt as expected
lineage for subsequent pages. It replaces candidate title/body with snapshot
fields; index/service text is only an ID hint. Current grants/publication and
lease verification remain mandatory after hydration. Page, query and neighbor
reads accumulate every inspected body before the result builder, including
nonmatching rows. A failure hidden by legacy count helpers is retained by the
capture and closes the result. The covered projection excludes generated
knowledge descriptions and arbitrary document metadata. Text chunks with
nonempty image/FAQ/custom enrichment are refused until their own writer /
hydration receipts exist. Exclusion never clears the inherited turn union.

## Adapter inventory

A complete empty envelope is an explicit producer assertion. The following
inventory describes this delta's coverage, not a promise that every registered
adapter has become usable. Code-owned static schemas remain available even for
an unused result adapter, allowing a fully accounted fresh text Agent turn.
Dynamic schemas need their own receipt before the model sees them.

| Actual adapter | Receipt / current behavior |
| --- | --- |
| Built-in default system/protocol prompts | Explicit code-owned literal producer; complete empty |
| Raw current human text / raw steer text | Explicit human-input producers; no resolved attachment/quote data on this path |
| Stored history / checkpoint / reasoning / tool arguments | Required input envelope from mandatory history loader; NULL/unknown closes before model |
| Custom system template, recalled memory, skill metadata | Required private setter receipt; nonempty unaccounted material closes |
| KB metadata, recent document titles, generated KB profiles | Required `KnowledgeBaseInfo.SourceLineage` over all rendered fields; no scope/name/channel heuristic |
| Selected docs, question origin, pinned MCP/skill descriptions | Required private producer receipts; document and origin receipts both retained |
| Raw user images / attachments / resolved quotes | `SetUserInputSourceLineage` receipt required; actual reader adapters still need integration |
| `thinking`, `todo_write` | Covered transformations of model control arguments and code-owned diagnostics; retain whole prior union |
| `read_document` metadata / text pages / text query / text neighbors | Covered actual database snapshots plus original exact lease; reader DI required |
| `read_document` FAQ, nonempty image/caption/custom chunk metadata | Closed pending derivative/body adapter receipts |
| `search_knowledge` (including rerank, FAQ answers, lazy child images) | Closed pending complete actual-reader receipts at every influencing model/enrichment boundary |
| `list_documents` | Closed pending snapshot pagination/filter/count provenance across all influencing document rows |
| `query_knowledge_graph` | Closed pending graph/entity/relation/body receipts |
| `search_conversations` | Closed pending generated-message search/partner/pagination receipt adapter |
| `search_memory` and recalled memory | Closed pending durable memory writer / retrieval provenance |
| `database_query`, `data_analysis`, `data_schema` | Closed pending materialization/query/control-result provenance |
| `web_search`, `web_fetch`, web-page `read_file` | Closed pending external adapter receipt; public-web status alone is not independence |
| Workspace/skill `read_file`, `list_sandbox_files` | Closed pending copied artifact/skill/source-file lineage |
| `write_sandbox_file`, `edit_sandbox_file`, `shell_exec`, `local_browser` | Closed pending actual workspace/process/browser input and output/artifact receipts |
| `write_skill_file`, `edit_skill_file` | Closed pending skill snapshot writer lineage |
| `wiki_read_page`, `wiki_search`, `wiki_write_page`, `wiki_replace_text`, `wiki_rename_page`, `wiki_delete_page`, `wiki_flag_issue`, `wiki_read_issue`, `wiki_update_issue` | Closed pending wiki/issue/backlink/derived writer and result receipts |
| MCP directory/proxy/direct/schema/instructions/OAuth/approval | Dynamic definitions/results closed; no opaque MCP execution starts. Incremental tool events require a typed receipt and engine emitter. Legacy direct approval bus hooks are not enabled by this delta. |
| Tool vision messages / VLM description | Inherit trusted result envelope; actual VLM send and response reauthorized. Opaque image producer remains closed. |
| Main round, planner/thought/function-call, final synthesis, retries/overflow | Actual wire messages are accounted before provider call; every output chunk and engine event rechecks current authorization |
| Compaction complete turns / split prefix / successive summaries / file-op annotations / raw archive | Union captured before fit/omission; summary message and checkpoint carry their own inherited envelope; revoked summarizer output cannot fall back to a readable archive |
| Fork / rewind / persisted derivatives | Producers preserve independent summary envelopes; actual fork, read, derivative writer, invalidation and durable stream proof remain separate service/repository work |

Unknown dominance and size/codec limits come from `types.SourceLineage`; the
engine never truncates a complete source set. Empty trusted tool results do
not clear older sources. Missing result-adapter capability closes *before*
execution; an empty opaque output cannot later certify independence. Tool
schema accounting uses exact trusted adapter types, never tool-name equality.

## Verification

Local models and source authorization services are stubs. Actual source rows,
revision ledger, source config/pair, build fences, original exact leases and
text/title snapshots use a private SQLite fixture. The tests cover source tool
→ citation-free answer → next turn → revoke, same-file two ETags/epochs, two
files with only one revoked, an empty result following prior source reads,
complete/split/successive/raw compaction, exact model output/event publication
revocation, unknown histories/dynamic prompts/images, explicit empty fresh
text, private JSON and actual `read_document` snapshot replacement.

Tests run in a uniquely named/labeled owned `weknora-go-test:1.26-sqlite`
container with network `none`, one CPU, 2 GiB memory, `GOMEMLIMIT=1800MiB`,
`go test -p 1` and a read-only source mount. Only that exact verified container
is removed; existing module/build caches are retained. This is source-entry
proof, not production runtime, PostgreSQL, HTTP/reconnect, actual fork, full
existing-suite migration, or model-provider evidence.
