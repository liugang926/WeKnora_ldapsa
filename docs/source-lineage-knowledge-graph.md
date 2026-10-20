# Original knowledge graph input receipts

Source candidate only. It does not deploy runtime code, certify all adapters,
close the PRD/V1 goal, or backfill old graph/configuration material.

The extraction task carries the original server admission proof. The handler
captures actual chunk/configuration rows under that proof, selects the typed
`graph_model_inputs` projection, and renders a detached system/user request and
model options. Every response inherits the full body/configuration input union,
including tags, example attributes, relation types and custom instructions.
The trusted input store rechecks the original scope, artifact/config digest,
monotonic chunk generation and full source tuples before model input, after
model output, before graph write and within the Neo4j write transaction.

The base extraction/entity protocol has a separate exact-value origin. Exact
embedded defaults are replaced by independent code literals. Arbitrary operator
YAML needs a private trusted human/derived origin; model and public graph JSON
cannot provide receipts. KB extraction configuration uses the separately
persisted configuration origin, so a new human document cannot authenticate
legacy examples as independent. Existing missing/malformed origins stay unknown.

## Immutable graph storage

Each extraction persists an immutable graph input header, node units and edge
units with original admission/generation, exhaustive union, input/config/wire
and exact namespace/body digests. Empty model output retains a header and its
input dependencies. Nodes/edges link to the original header. No name merge
rewrites an older generation or promotes an unknown legacy row.

Neo4j reads headers and raw contributing records within the read transaction.
The bounded query accounts all actual namespace candidates before name
predicates, including nonmatching names and empty-result controls. It validates
raw body/receipt/header bindings before presentation collapse. Missing,
malformed, over-limit, changed and orphaned material closes the whole result.
Equal display names from multiple documents retain every original dependency.

## Readers and serving boundaries

`KnowledgeGraphLineageReader` rechecks each persisted original input proof,
current document/scope and the complete source union. It acquires server KB
leases, pins all underlying controlled sources (including configuration/example
sources), and hydrates endpoint text/title through the actual chunk snapshot
reader with the original expected lineage. Current chunks do not certify old
node attributes, relation types or controls. The returned private snapshot
retains lease/check/close functions through tool/pipeline formatting.

The Agent adapter calls the actual graph reader, emits actual graph facts and
relations, and unions graph controls plus every hydrated chunk before
name/chunk deduplication. The model projection keeps graph attributes/edges
alongside registered chunk/document handles. The ordinary `search_entity`
consumer uses the same reader and retains the entire graph union in the request
accumulator even when no new endpoint chunk is appended. Entity extraction
uses the actual detached question/protocol input envelope and checks before
and after its model call. Source-independent generated descriptions that the
graph model never saw do not replace its original proof.

Original interactive caller and effective source tenant remain separate. The
existing request-local verifier owns current Agent, organization, directory,
KB and live source permission decisions. Every model/event/history boundary
continues to require the complete inherited envelope; citations cannot reduce
it. The graph producer does not invent authority from a name/channel/client
JSON or a current endpoint lookup.

## Verification scope

Tests use local model stubs and a local signed Nextcloud publication protocol
stub. SQLite stores real admissions/config origins/material receipts, source
revision/publication rows and durable original build/read leases. An owned
Neo4j 5.26.31 fixture uses the pinned digest
`neo4j@sha256:c7d25c0eeebfe125718d58b72ed5d663c85d0479047733845eb2237c67ce8069`,
one CPU, 512 MiB, 128 MiB heap and 64 MiB page cache, an internal network and
no host ports. Go tests use `weknora-go-test:1.26-sqlite`, one CPU, 2 GiB,
read-only source/module mounts and the retained shared build cache. Cleanup
checks exact IDs, image, owner/nonce, mounts and network peers before removing
only those owned resources; it never prunes or changes shared databases.

Covered entry cases include actual extractor -> SQL proof -> Neo4j -> Agent /
ordinary reader, actual custom configuration input, graph attributes/relations
in model projection, citation-free answer -> next turn -> source withdrawal,
multiple-document name merge with withdrawal during read, two original epochs /
ETags, old tuples unable to borrow current chunk proof, model/write withdrawal,
whole-transaction rollback, configuration/content ABA, hard deletion,
legacy/changed/orphan graph material, empty extraction headers, complete empty
fresh inputs, and callback mutation of producer-owned values.

Uncovered product acceptance remains real AD/Nextcloud authorization, external
model providers, production PostgreSQL plus Neo4j serving, HTTP/SSE reconnect,
actual deployment, every derived artifact/wiki/memory/attachment adapter and
comprehensive runtime build/index/GC inventory. Existing frozen source trees,
shared Nextcloud/runtime deployments and main branches are not modified here.
