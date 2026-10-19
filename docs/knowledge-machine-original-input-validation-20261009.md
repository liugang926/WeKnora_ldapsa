# Machine producer original inputs — 150 / SQLite 69

Base: `7c41395cf01048a5fff4d52300551e1f9a7e5bb4`. This independent delta does not edit the root candidate or template freeze `42469bea6a87caeae666e9d05764ed5e1026bd18`.

## Production boundaries

- The real signed Nextcloud connector seals its actual content bytes, original tuple/ETag/name/path and source configuration digest in an opaque capability. JSON or a queued job cannot construct it. Sync captures monotonic source, pairing, name, KB configuration/routing and tenant controls before transport. File creation verifies them before storage and atomically persists the immutable original upload receipt with the new knowledge row.
- The actual document worker requires that receipt and its exact private build lease before parser/span writes. It compares stored bytes and task controls against the original receipt; it cannot authenticate a legacy file by reading its current bytes. Grouping tags consume the immutable initial name/configuration proof after Stage. Additional controlled tag/configuration sources retain public current actor/source authorization.
- Summary and Question complete before the new Auto producer captures final actual material. Auto seals its actual queue payload, all candidate/relationship/name controls, actual model/tenant configuration digest and monotonic revisions. Tenant credentials are runtime-only (`json:"-"`), and never enter this immutable JSON or the classification prompt.
- Question output uses an atomic metadata-only writer. Its immutable same-admission transition permits its own original span material to survive the legitimate output. External metadata/body edits and ABA restorations have no such transition and are rejected.
- Full summary text is removed from ordinary logs; count/scope/tracing remain. Guarded span preview/sample output is still controlled body and is handled by the separate retention module.

## Actual verification

All runs used a read-only source mount, the pinned Go image, 1 CPU / 2 GiB and the existing module/build cache volumes. Models are isolated mocks. The actual pipeline uses production `ProcessSync`, signed connector transport, `CreateKnowledgeFromFile`, storage resource catalog/capacity admission, original admission, Stage, Asynq handler paths, `SimpleFormatReader`, SQL chunk/index writes, Summary, Question, Auto and publication. KnowledgeService and StageRepo are not stubbed.

| Run | Actual result | Scope |
| --- | --- | --- |
| r14 | 33 pass / 0 fail / 0 skip | First complete SQLite/PG machine chain and original input negatives. |
| r15 | 49 pass / 0 fail / 0 skip | Adds extra public-source authorization, actual actor revoke, model/tenant ABA and forged queue controls. |
| r18 | 11 pass / 0 fail / 0 skip | Actual saved QA/profile, public chunk reads, own-output span transition and name edit/revoke boundaries. |
| r19 | 196 pass / 7 fail / 11 skip | Preserved historical regression failure: old auto fixture lacked actual model/tenant catalog, capacity used a handcrafted item, PG DSNs absent. |
| r20 | 9 pass / 0 fail / 0 skip | SQLite/PG credential digest-only persistence, SQL credential ABA and modified runtime tenant context. |
| r21 | 225 pass / 5 fail / 0 skip | Four-package combined regression. Machine tests: 74 pass. Remaining failure was old Auto receipt count expecting one instead of the new queue receipt plus model receipt, including parent/package fail nodes. |
| r22 | 17 pass / 0 fail / 0 skip | Corrected assertion verifies exactly one actual queue receipt and one actual model receipt; complete Auto actor/ABA/forged negatives and SQLite/PG Summary-to-Auto published reads pass. |
| r23 | 115 package pass / 39 no-test-package skip / 0 fail | Whole repository compile (`go test -p 1 -json ./... -run '^$'`), terminal exit 0. No test was skipped by a runtime fixture. |
| r24 | 2 pass / 0 fail / 0 skip | Final actual SQLite Summary→Auto→publish→guarded metadata/chunk/RAG positive after removal of the summary body log field; terminal exit 0. `GetSummary success` contains `has_profile=false summary_bytes=32` only. |

r9 only proved Auto with Summary skipped; r11/r13 first proved Summary model plus Auto. r16 exposed missing persisted Question metadata; r17 then exposed its span self-invalidation. These runs are historical failures, not positive evidence. The final real QA output and external metadata/body ABA checks are in r18/r21.

Original stdout JSONL artifacts remain at `/tmp/weknora-machine-*-c6-20261009-rN.jsonl`; the adjacent committed evidence JSON records their actual SHA-256 and terminal actions. It excludes body/credential-bearing stdout. Exact final tests live in `knowledge_machine_pipeline_test.go`, `knowledge_machine_question_span_regression_test.go` and `knowledge_machine_secret_controls_test.go`.

## Required root DI

After the existing provenance store and transaction-scoped display authority factory are available:

1. `repository.ConfigureDataSourceNameInputWriters(dataSourceRepo, nextcloudSourcePairingRepo, displayAuthorityFactory)`.
2. `service.ConfigureDataSourceOriginalInputSources(dataSourceService, knowledgeInputStore)`.
3. `service.ConfigureKnowledgeAutoTagCompletion(postProcessHandler, knowledgeRepo, knowledgeInputStore)`.

The factory must construct authority from the supplied transaction and retain current actor/group checks. Keep the existing original build-purpose source verifier and public guard wiring. Apply PostgreSQL 150 / SQLite 69; template 147 / SQLite 66 remains an independent root merge.

## Owned fixture cleanup and remaining integration

Owned PG container `weknora-machine-pg-c6-20261009-d134`, ID `5ec47541c660c406925f41e4d75844f2f8bc937e24c1a78cb4f708977bf4ea75`: pinned PG16 image; `AutoRemove=true`, 1 CPU / 512 MiB, tmpfs 256 MiB, empty host port bindings. Owned network `weknora-machine-c6-20261009-d134` was internal, ID `48a2930e653c78a8a4091db5ebd24b6ffbc27b69f663003f3775c71a632aa91f`. Actual final SQL query counted zero non-system schemas. `docker stop` removed the container; `docker network rm` removed that exact network. Final filtered inventory contains no machine fixture containers/networks. Cache volumes are retained; no other fixture was touched.

Root still needs to combine this delta with its current source/GC/message/template changes and run the complete local application acceptance. This delta's full machine pipeline positive is Markdown; the independent phase-1 image worker positive is separate evidence. No real enterprise directory or paid model was used. Auto queue delivery retains the pre-existing best-effort Redis/SQL handoff rather than adding a durable outbox in this provenance delta.
