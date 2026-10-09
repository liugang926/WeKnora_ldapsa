# Wiki source scope policy

Every public Wiki service reader and mutator now requires the production knowledge-base source policy before repository access. Missing or unsupported policy refuses the request. Source-marked KBs remain unavailable to Wiki, including revisions, source summaries, cursor listings, link repair, folders and internal Agent callers. This implements the V1 restriction; it does not certify historical Wiki bodies or external indexes for collection.

ID-based reads first select only `knowledge_base_id`, check the policy, then hydrate the page and verify its ownership did not change. Ordinary Wiki fixtures now explicitly supply a scope policy. Actual SQLite tests verify a denied ID read issued one ownership-only query, no full body query, rejected updates leave the body unchanged, and an ordinary KB remains usable.

The first broad regex run included unrelated post-processing summary tests whose existing fixture lacks admitted prompt origins; it had three failure nodes. Those raw logs remain private. A focused Wiki/source-policy run passed 36 test/subtest cases, zero failures/skips across service and repository packages. The three original revision restore/churn/delete tests also passed with no failures or skips. No shared application was switched and no production GC coverage marker was created.

Metadata updates also resolve the actual stored ID ownership and reject a caller-supplied KB mismatch. The repository predicate repeats the ID/KB match. Six focused policy/ID-owner cases passed, including a forged ordinary KB that cannot mutate the protected row.
