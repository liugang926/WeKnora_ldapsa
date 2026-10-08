# FAQ, tag and collection HTTP read fences

FAQ list, entry, search, export and producer responses now acquire a durable
read lease on the selected KB before loading the FAQ input snapshot. Tag list
and committed producer responses do the same. The lease remains live through
predicate/count evaluation and actual HTTP writes, renews with a bounded TTL,
and is released on success, rejection or cancellation. Original input receipts
and current source/actor checks still authorize every result; a lease grants no
permission. A missing durable store returns unavailable.

The existing knowledge collection handler already fences every resolved target
KB before capture. Its missing-store path now refuses the request instead of
continuing without the fence. All three paths use the same underlying database
as their original-input receipts and the physical GC claim protocol.

Actual SQL/HTTP contracts verify that a retired generation cannot obtain a GC
claim while these requests hold their broad KB lease, that the claim succeeds
after the response releases the lease, and that lease expiration, retired KBs,
cancelled requests and absent stores suppress output. The existing signed-source
withdrawal and current-actor tests also remain necessary.

This establishes the selected KB boundary. Additional original parent chunks
can belong to other KBs, and those scopes must be resolved from immutable
receipt metadata and leased before parent material is captured. Full reader /
builder inventory and physical adapter coverage remain outstanding. This change
does not insert production lease-coverage rows or enable derived GC.

The tested GC competitor is the generation claim protocol. The existing
`RunDue` local-object claim adapter still needs to join this content fence;
its resource-reference lock alone does not establish reader/build coverage.
Actual provider deletion and that adapter's races are a separate pending gate.
