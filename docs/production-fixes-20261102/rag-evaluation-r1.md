# R1: Evaluation configuration admission and error display

Baseline: 9e574d60f1c26ab6286e1236d840b9b56fa41cfc (verified against production build identity).

Both evaluation KB creation branches now use one scoped producer. It creates only the fixed `evaluation` name/description and selected model identifiers. A reference KB supplies only the embedding identifier; no reference name, templates, prompts, passages or processing configuration is copied. The fixed generated configuration receives a complete empty derived lineage, and the exact new name uses the existing current-actor display admission. Repository and authorization checks remain unchanged. The context is scoped to creation, not propagated as blanket admission to subsequent evaluation work.

The evaluation page now extracts textual messages from structured HTTP errors or Error instances; malformed objects get a readable fallback instead of object coercion. All error boundaries on this page use the helper.

## Validation

- `frontend/node_modules/.bin/tsx --test frontend/src/views/settings/evaluationError.test.ts`: passed (run from repository root or adjust relative path).
- `cd frontend && npm run build`: passed, existing chunk-size warnings only. Existing local frontend dependency cache was symlinked; no lockfile changes.
- Remote isolated source `/tmp/rag-evaluation-r1-20261102` with Go `/tmp/go1.26.0/go/bin/go`, `GOPATH=/tmp/weknora-gopath GOCACHE=/tmp/weknora-gocache GOMAXPROCS=4 GOTOOLCHAIN=local`:
  `go test ./internal/application/service -run 'TestEvaluationConfigurationAdmitsOnlyFreshFixedConfiguration|TestSyntheticChineseBenchmark' -count=1`: passed.
- New regression uses real SQLite provenance writer, current web actor/group authority and display schema; checks fixed configuration admission and verifies unadmitted writes remain rejected. Initial test fixture lacked display authority/schema; fixture was corrected without changing production checks.

## Deployment boundary

No production service/config/data was changed. Build the app and frontend from the final accepted combined commit using the existing production release/image identity process. Preserve `current`, resolved image digests, existing environment/volumes and updater persistence. Do not overwrite concurrent DingTalk SSO or unrelated releases. Record prior image digests and current release for rollback; rollback restores prior app/frontend image selections through the same release process. No database migration is required for R1.

Do not deploy R1 as an end-to-end evaluation fix alone. R2 must admit local benchmark passage ingestion at `EvalDataset` -> `CreateKnowledgeFromPassageSync` -> `createAdmittedKnowledge` and independently test task execution. R1 intentionally does not alter this separate work unit. Full production acceptance must create one controlled synthetic evaluation, await final status and confirm case/metric results; merely returning a task ID is insufficient.
