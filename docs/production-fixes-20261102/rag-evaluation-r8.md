# R8 wait for persisted evaluation corpus readiness

Production evidence: the failed request queued postprocessing at 18:26:53.831,
started QA at 53.833, reached finalizing with one pending subtask at 54.557,
and indexed its summary at 18:27:00.759. QA attempted strict chunk snapshots
before finalization. Sync passage import only waits for indexes; snapshot
hydration deliberately requires completed/enabled knowledge. The subsequent
summary decrement error overlapped failed-evaluation cleanup, so it is not
proven to be an independent source defect.

EvalDataset now waits on actual tenant-scoped repository reads after installing
cleanup and before starting any QA worker. It requires the exact knowledge/KB/
tenant identity, enabled state, completed parsing and zero pending subtasks.
Processing/finalizing are polled once per second, bounded to five minutes;
failed/cancelled/deleting/disabled/missing or changed identity and context
cancellation fail explicitly. No snapshot or authorization rule is weakened.
Both cleanup defers use a separate 30-second WithoutCancel context preserving
caller values, so cancellation does not skip deletion of the exact temporary
resources already recorded by this run. No other identities are enumerated.

The regression uses actual SQLite ingestion, original-body receipts, group
policy and the strict Search/Rerank/Merge/IntoChatMessage/ChatCompletion chain.
A real persisted document in processing reproduces the snapshot refusal;
the wait stays blocked through finalizing and only permits the same strict QA
when completed with zero pending work. The answer is nonempty. Timeout,
cancellation, failed/deleting/cancelled, incomplete subtask count, disabled and
wrong KB/tenant are rejected. Embedding/candidate scoring and final model are
controlled substitutes; this does not replace real production evaluation.
The old corpus-admission fixture now represents finalized imports rather than
an indefinitely pending record.

Remote test source: /tmp/rag-readiness-r8-20261102
Go SDK: /tmp/go1.26.0/go/bin/go; GOPATH=/tmp/weknora-gopath;
GOCACHE=/tmp/weknora-gocache; GOMAXPROCS=4; GOTOOLCHAIN=local.
Test regex: TestEvaluationReadinessActualPipeline|TestEvaluationLocalDatasetAdmissionAndCompletion|TestEvaluationConfigurationAdmitsOnlyFreshFixedConfiguration
Use -count=1 -timeout 3m. Final log: /tmp/r8-test-final.log.
No production changes, evaluation requests or model calls were made.
