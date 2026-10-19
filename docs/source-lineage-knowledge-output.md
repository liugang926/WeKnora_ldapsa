# Knowledge output material receipts (private candidate)

This module is staged source work. It is not a runtime deployment or full
consumer/PRD acceptance.

Fresh metadata reads start with the actual material receipt's original
AdmissionID, never the newest admission, current channel or absence of source
rows. A repeatable PostgreSQL snapshot (SQLite read transaction) checks the
immutable origin, row revision, body digest, original source generation and
config. The result includes actual persisted fields and its original proof.
The worker capture API still rejects missing original task proof; this output
reader does not recertify supplied text or old tasks.

A response guard detaches its initial proof, stores the exact wire fields,
authorizes all captured source and KB dependencies, and checks the complete
original material set in one database snapshot. It retains the initial
admission at later output boundaries, including when equal current bytes gain
a new admission. Mutable callbacks cannot alter retained authority or the
wire body after authorization. The knowledge HTTP lease context carries this
guard through JSON writes, including ordinary rows without a Nextcloud lease.

The production candidate DI shares one actual provenance store with the RAG
reader, admitted writer and knowledge output authorizer. Joined tag/KB display
text, free-form error body and FAQ reports do not inherit a document's origin
without their own producer receipt. Unknown material remains closed.

On the first staged c6/core558 combination, 22 repository/service tests and
seven actual HTTP tests/subtests passed, without models or external network.
They cover exact receipt versus a newer unrelated origin, legacy and foreign
rows, material ABA, original-admission retention, detached callbacks, actual
human metadata delivery, unknown metadata rejection, current group withdrawal,
wire alias changes and database ABA before response. Four packages compiled.
The broader service vet gate found pre-existing unreachable memory-injection
code in the cumulative RAG delta; its owner is fixing that code. These results
are not proof of the later config-origin/projection combination.

Further required output producers include chunk bodies, actual file bytes,
historical processing spans, list/search selection and counts, tags, display
text and generated reports. Metadata authorization alone does not certify
those distinct bodies or their influences. Current source proofs also do not
establish complete physical-GC read coverage.
