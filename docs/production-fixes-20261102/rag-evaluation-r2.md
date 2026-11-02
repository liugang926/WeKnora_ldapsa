# R2: Local benchmark corpus admission

Base R1: 7264203f2. This unit modifies only evaluation service admission and adds a focused integration regression.

The concrete DatasetService is the existing operator-controlled, de-identified local Parquet benchmark producer. Its bounded ID/path checks and relation validation run before admission. A replacement/unknown DatasetService is rejected; arbitrary business knowledge is never relabeled independent. The loaded corpus receives a complete derived input envelope only for CreateKnowledgeFromPassageSync; the repository binds the actual sanitized corpus digest and actor to the original input proof. The envelope is not placed on the QA context. Existing read/lifecycle/ACL checks remain intact. No reference knowledge base body, name, prompt or configuration is read into the benchmark.

The existing async logger.CloneContext(context.WithoutCancel(ctx)) retains the current actor needed by the real provenance writer; this is exercised by the integration test. No global context cloning or source policy was changed.

Validation on isolated /tmp/rag-evaluation-r2-20261102 (user-owned source and SQLite test databases, no production data):

```
GOMAXPROCS=4 GOTOOLCHAIN=local GOPATH=/tmp/weknora-gopath GOCACHE=/tmp/weknora-gocache \
/tmp/go1.26.0/go/bin/go test ./internal/application/service \
-run 'TestEvaluationLocalDatasetAdmissionAndCompletion|TestEvaluationConfigurationAdmitsOnlyFreshFixedConfiguration|TestSyntheticChineseBenchmark' -count=1
```

Passed (1.213s). Log: /tmp/rag-evaluation-r2-test.log. The new test uses the concrete local dataset loader, real SQLite configuration/body provenance writers, real durable evaluation repository and production actor detach; imported 16 passages, executed EvalDataset's 16 cases and persisted all nonempty answers and the dataset fingerprint. External model/QA execution and indexing are test doubles: this is not a claim of real model end-to-end validation. Raw unadmitted corpus writes remain rejected; missing loader and unsafe dataset ID are rejected. R1's configuration tests also pass. Initial test-only cleanup double lacked GetRepository and panicked after the 16 cases; the fixture was corrected to use the real repository, with no production policy change.

Deployment: combine R1+R2 app build and R1 UI build. No schema migration. Independent tests and a controlled production synthetic evaluation must verify real indexing/models, final task success, case answers and temporary KB cleanup before declaring complete. Preserve current release/image identity and rollback through the normal release process. No production service, configuration or data was modified here.
