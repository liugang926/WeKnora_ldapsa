# Nextcloud source provenance

The `knowledge_bases.ever_had_nextcloud_source` flag records that a knowledge base has contained a Nextcloud source. It is set in the same database transaction that creates or changes a data source to `nextcloud`, and application code never clears it. Deleting the data source and its knowledge rows therefore does not make previously generated Wiki pages or shared agents appear independent of the source.

Data-source creation and updates serialize on the owning knowledge-base row. A Nextcloud source requires a dedicated knowledge base with no other source or knowledge row, including soft-deleted rows. Once Nextcloud provenance is set, ordinary sources cannot be added to that knowledge base. A Nextcloud source cannot change its tenant, knowledge base, or connector type through the update path.

Migration `000111` for PostgreSQL and `000030` for SQLite backfill the flag from current and soft-deleted data sources and knowledge rows. **A source and all of its knowledge rows that were hard-deleted before this migration leave no reliable provenance to backfill.** Operators should inspect such older knowledge bases and remove or quarantine their derived Wiki pages and shares before enabling access. Subsequent Nextcloud source creation is covered by the transaction-backed flag.

Knowledge bases with this flag are conservatively ineligible for cross-knowledge-base copies, organization KB shares, shared agents, and Wiki reads/generation. Ordinary knowledge bases retain these features.
