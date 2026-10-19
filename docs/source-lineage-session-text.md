# Session text lineage

Private source-only phase; no runtime deployment or full PRD completion claim.

PostgreSQL 133 / SQLite 52 add nullable title_source_lineage and
description_source_lineage, plus a private monotonic title_revision. Legacy NULL remains unknown. No absence of source
rows, channel or ever-source heuristic certifies legacy text. Downgrades stop
for an explicit audit export rather than erase original provenance.

Repository rows keep raw private text and proof. Session JSON defaults to
redacting both fields even for a raw complete-empty row until the service marks
authorization. Get/list/paged/by-ID readers preserve existing session ownership
rules and authorize each complete field. Complete source-bearing fields use the
current lineage authorizer; denied fields become generic empty text and
availability failures stop the operation. Single-field collections and lists
perform a final batch check after all remote calls: each original Agent/KB
scope is retained, and the complete source union and current directory identity
are rechecked after the last scope lookup. This protects earlier content from
a source/fence or identity change committed during a later authorization call.
Raw repository objects are not mutated by redaction.

Keyword search first excludes unknown proof candidates in SQL, then authorizes
all candidate titles before pagination and total/count. Denied or unknown titles
cannot affect matches. Without a keyword, ordinary owner-visible session rows
remain enumerable with redacted generated text. The candidate reader currently
loads the matching owned rows to compute authorization-aware totals; a bounded
cursor implementation is a future scalability improvement, not an exemption.

Manual HTTP create/update explicitly stamp raw caller-provided text as
source-independent. Reserved description preservation inherits the original
proof instead of recertifying a retained marker. The title model sends only raw
user Content and a static configured title prompt, checks before and after the
call, and saves title plus original input proof in a single owner-scoped CAS
statement. The compare binds the original text, original proof, monotonic DB
revision and tenant/owner. Database triggers advance the revision for every
title/proof write, including direct SQL same-text and ABA edits; timestamp
resolution cannot restore a stale comparison. PostgreSQL replaces caller-set
revisions with the next value; SQLite rejects attempts to reset the revision.
A manual edit during the model call wins. A failed write neither
publishes the new title nor replaces its previous committed provenance.

Async title events reload the committed text and its original proof after
GenerateTitle, bind the returned text and reauthorize that snapshot. The return
value may be an existing source-bearing title, so an event cannot assume it was
created from today's source-independent raw query. Its exact original envelope
is carried to the stream sink; unknown, changed or revoked text is not emitted.

Fork retains the original title/description proof unchanged for inherited text;
unknown inherited text stays redacted. An explicitly requested human fork title
is source-independent. Message search carries session-title proof privately and
checks it before including the title. History and message search retain every
answer's original Agent/KB scope in the final batch, rather than reducing those
permissions to an empty scope for a source union. Image/artifact body producers remain
unknown and closed under the message reader from the preceding RAG delta.

Focused tests cover raw default JSON/redaction/privacy and IM fields, legacy
NULL/unknown read behavior, atomic title/CAS and injected write failure,
unknown keyword candidate exclusion without backfill, actual title model input
receipts and different/same-text/ABA manual-edit races, later-read revocation,
SQLite 51→52 and PostgreSQL 132→133 real upgrades. Both dialect upgrades verify
monotonic revisions and attempted reset. The actual PostgreSQL repository test
also covers original NULL/complete proof, owner and stale revision comparisons.
Full SQLite 0→52 terminal gates assert all three new columns. PostgreSQL full-chain
tests already compute the terminal migration from filenames; the metadata
upgrade test uses the supported embedding-skip mode on disposable PG16.
