# Generated event producer receipts

The stream sink accepts an explicit typed original SourceLineage attached by
the producer of each generated event. It never supplies a missing or invalid
receipt from the current RAG runtime: that runtime cannot prove which inputs
an uncovered event body actually consumed. Missing, wrong-type or unknown
metadata poisons the prefix and stops durable output before it is appended.

The only receiptless close operation seals an existing complete prefix without
new body, references, steps, usage or extra metadata. Its session/request/message
IDs must match the bound stream; all other complete events need their own typed
producer proof. A new final answer does not qualify for this close operation.

Producer receipts remain internal. Durable stream frames retain a monotonically
growing source union and public serialization omits that inventory. Current
publication and replay checks still run at their respective boundaries; this
receipt validation does not replace those checks or the saved-answer proof.

Actual EventBus/stream-handler regressions verify a complete-empty RAG runtime
cannot authorize a receiptless body or wrong-type receipt, a close operation
cannot carry new references/extra fields, and a plain close can seal an already
accounted prefix. The actual async title producer retains an existing title's
source-bearing proof and rejects stale/legacy/revoked snapshots. These are
private source increments, not full adapter coverage or runtime deployment.
