# Installed body journal + automatic completion combination review

Fixture freeze: `7dd09bcd67830bdbcf745f8e74f83bd3f450178c`, based on the root pre-integration tree `d7cee0415eeafadbbb1507ef6ea63a2c2362aa2d`. Only three test files change; the root tree was read-only throughout this review.

## Concrete gaps repaired

1. The actual machine fixture omitted PostgreSQL152/SQLite71 while loading Auto153/72. Its old positive therefore did not exercise installed original-body callbacks or the external journal. The fixture now applies both migrations before real source/name/queue/model production, creates an owned 0700 journal directory and 0600 key, calls the actual initialization/readiness/trigger/callback functions, and sets PostgreSQL/SQLite pools to one. Actual pipeline positives assert two captured Auto body heads, one for the queue manifest and one for the model manifest.
2. The SQLite migration expectation stayed at70. It is now72, and schema parity asserts the body payload/head/revision/lease/rewrite/journal/purge/GC tables plus both exact Auto publication tables. Actual upgrade tests and directory loading pass.
3. A durability assertion selected only actual_inputs into an anonymous partial projection and expected raw text. Once152 is installed, that SQL value correctly becomes an opaque body reference. A named DTO with manifest ID and full tenant/KB/knowledge scope now uses the actual Query resolver; original generated profile and questions are still asserted after resolution. No reader or source gate was weakened, and no old empty-head vault row was inventoried or re-signed by this fixture.

The same-transaction factory remains intact: container construction calls NewKnowledgeInputProvenanceStore(tx), whose default constructor installs the body callbacks and binds the availability checker. No static omission in this factory was found. Journal initialization is intentionally explicit; the runtime VerifyJournalReady startup gate must remain. The parent owns its local Docker --body-journal provisioning/startup fixture and deployment acceptance.

## Actual results and limits

- r4: **53 pass / 0 fail / 0 skip**, terminal0. All Auto durability cases run with the body protocol installed, plus SQLite migration tail/schema/upgrade checks.
- r5: **69 pass / 0 fail / 0 skip**, terminal0. Original machine Summary/QA/Auto/publish, original input mutations, inherited public-source/actor authorization, span mutations and credential controls run with the body protocol installed.
- r6: **13 pass / 0 fail / 0 skip**, terminal0. After importing consumer7930e9009296f895e27a234a86b5d649041bd428 and the metadata-only/empty-vault body fixes c201507df8ec6cd73294ceef22b12f5aa8b2bfc8 / 58e163415220d85a9798021da07a41d39da3a537, a small dual-SQL actual pipeline/durability/zero-subtask regression passed at local HEAD0f9664937207ce0c42c7d847b1fac61da8183045. The separate message owner owns message material and same-transaction bridge tests.

All package event counts include parent test/subtest/package events. PostgreSQL16 and file-backed SQLite are real; both use pool1. Production signed ProcessSync, KnowledgeService, parser/chunk/summary/QA/completion/Auto/publisher methods and the body journal run. Models and the existing retrieval fixture backend are isolated implementations, with real Composite selection. These are package handlers under real leases; whole-app background workers, real enterprise AD/Nextcloud and paid models remain parent acceptance boundaries.

Historical logs are preserved: r1 is WIP diagnostic coverage; r2 contains the old partial-projection assertions; r3 hit the aggregate service package300s timeout after64 pass events, with no test assertion failure. The required sets were split into serial bounded runs r4 and r5. The adjacent JSON records every stdout SHA-256 and terminal without copying body-bearing stdout.

Minimal revalidation after applying the fixture commit:

```sh
go test -tags sqlite_fts5 -p 1 ./internal/application/service ./internal/database -run 'KnowledgeAutoCompletion|MigrationDirectoriesLoad|SQLiteMigrations|SQLiteSourceLineageUpgrade|SQLiteNextcloudRevisionUpgrade' -count=1 -timeout=300s
go test -tags sqlite_fts5 -p 1 ./internal/application/service -run 'KnowledgeMachine|PostgresKnowledgeMachine' -count=1 -timeout=300s
```

Supply an owned WEKNORA_AUTO_TAG_TEST_POSTGRES_DSN and WEKNORA_AUTO_COMPLETION_TEST_REDIS_ADDR. The fixture provisions its own private journal per test. Run each command serially at1CPU/2GiB, with source mounted read-only and an internal network with no host ports.

## Owned cleanup

PG/Redis/internal-network exact IDs are recorded in JSON. Both AutoRemove containers were stopped and the exact network removed; owner-filtered inventories are empty. Stop-before-cleanup SQL counted **one** non-system schema; zero was not established, and its origin was not inspected. The entire owned PG container and its tmpfs data were removed, so this residual owned schema has no persistent fixture storage. Existing weknora-go-mod / weknora-go-build volumes were retained; shared services were untouched.
