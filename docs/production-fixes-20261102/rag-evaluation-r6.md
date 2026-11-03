# R6 evaluation target tenant correction

The production evaluation constructed a knowledge-base SearchTarget without
TenantID. NextcloudSourceLineageGuard.checkCurrentKBReads rejects any target
whose TenantID is zero before retrieval. The strict event entry is correct;
the evaluation adapter omitted required identity.

The only production change fills SearchTarget.TenantID from the durable
EvaluationTask tenant. No new authorization entry, interface, guard exception,
source receipt or ACL mode is introduced. The generic strict pipeline stays
unchanged, including current actor, KB ownership, group and lifecycle checks.

The regression uses actual SQLite source ingestion, original-body receipts,
chunk hydration, real GroupAccess/current-user eligibility, the strict session
event entry and real Search/Rerank/Merge/IntoChatMessage/ChatCompletion plugins.
Only vector candidate scoring/embedding and model responses are controlled
fixtures. It demonstrates zero target tenant rejection, then successful
retrieval and a nonempty answer with the target tenant populated. Negative
cases cover absent actor, forged target tenant, extra unauthorized target,
explicit image, disabled actor and missing KB.

Remote isolated test source: /tmp/rag-eval-qa-r6-20261102.
Go: /tmp/go1.26.0/go/bin/go; GOPATH=/tmp/weknora-gopath;
GOCACHE=/tmp/weknora-gocache; GOMAXPROCS=4; GOTOOLCHAIN=local.
Target: TestEvaluationQATargetTenantActualPipeline, -count=1 -timeout 4m.
No production files/data/services or model calls were changed by this unit.

Build against the cached reviewed AnyDoc builder; the binary must be rebuilt
for this source commit. Preserve R4 packaged benchmarks in the final runtime.
Independent tests and a new real production evaluation are still required.

## Self-test results

- TestEvaluationQATargetTenantActualPipeline: PASS, seven subcases, 105.450s.
- Existing evaluation configuration/dataset and KnowledgeQAAuthorizer regressions:
  PASS, 1.237s.
- git diff --check: PASS.

During fixture development, the test required wiring the same current-user
eligibility service as production. A proposed direct paired-KB deletion was
correctly rejected by the existing DB lifecycle trigger, so the missing-KB
negative case uses an absent identity. The forged-tenant test now changes the
target tenant, rather than leaving the original authorized caller and target
while only changing a separate context value. No production guard was relaxed
in response to any test result.
