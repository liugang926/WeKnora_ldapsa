# RAG retrieval physical read lifetime

Actual search and augmentation acquire KB/exact read guards before index or
SQL body access. A QA/Agent turn registers those guards with the existing
private model/HTTP holder. The shared output checker renews/verifies each
guard; only the last producer/sender reference releases it. One-call reads
outside a turn retain their existing method lifetime.

The turn reuses guards it owns. A borrowed, shorter-lived guard supplies
request authority values and original request cancellation but does not own
the turn's new fence. Closing that borrowed guard therefore cannot release
the longer turn fence. Current source/actor authorization remains separate.

Frozen production source d6e1b4f620a7cfb293001c6c6e1eeee5836cb6b0 passed14
tests/subtests with zero failures/skips. These exercise actual file-backed
SQLite/pool1, original document/chunk writers, actual RAG SQL hydration,
producer/sender references, borrowed-parent closure, expiry/cancellation and
existing retirement/concurrent-close regressions. The first diagnostic
failed because the fixture omitted the original creation receipt when
capturing build inputs; that failure is retained. No gate was weakened.

This checkpoint proves hydration/reference lifetime and shared checkpoints.
It does not yet prove full model generation, public HTTP integration,
PostgreSQL for the new module, every reader's GC coverage or normal Docker
application acceptance. Those remain combined-source gates. The JSON records
all observed process terminals and log hashes; shared caches are preserved.
