# Nextcloud event receiver and connection provisioning

`POST /api/v1/integrations/nextcloud/events` persists change hints in WeKnora.
The route is independently authenticated before user and API key middleware.
An HTTP `202` means the entire batch and its receipt watermark committed to the
database. It does **not** mean that WeKnora reconciled the source, parsed a
document, published a version, or applied a deletion. A background dispatcher
submits source reconciliation jobs from committed inbox rows. The applied
watermark advances only after WeKnora proves a complete publication run;
HTTP `202` is never an application acknowledgement.

Only an `active` connection row on an `active` data source may receive events.
Pausing a source stops new durable receipts even when its connection remains
active. Indexed source withdrawal revokes that connection and blocks dispatch
in the same database transaction as the source pause; subsequent signed
batches cannot receive HTTP `202`. The row fixes one
Nextcloud instance and binding to one tenant, knowledge base, and Nextcloud
data source. The data source must still select exactly that binding. Pairing
and key provisioning use the administrator endpoints below. Installing the
receiver alone does not create an active connection. Connection secrets are
random and stored only as `enc:v1:` ciphertext using `SYSTEM_AES_KEY`.
The connection also pins the normalized complete Nextcloud base URL and the
SHA-256 fingerprint of the data source's **committed, encrypted** configuration
bytes. An endpoint edit, credential clear/reprovision, or unrelated
configuration edit invalidates the old connection and requires re-pairing.
A finalized, exact source machine-key rotation can instead rebind the existing
connection through the administrator route below.
The receiver checks this identity inside the same locked transaction as the
inbox write; it does not hash or log a plaintext machine token.

## Administrator connection lifecycle

The following routes require the current tenant's Admin or Owner role and
edit access to the target knowledge base. Tenant API keys do not issue or
recover event secrets. The target must be a committed, dedicated Nextcloud
data source whose sole selected binding still exists in signed live
capabilities/bindings responses. Pairing checks the persisted tenant, KB,
source, base URL, encrypted config fingerprint, and dedicated KB marker again
under the database lock before committing the connection.

| Method | Route | Result |
| --- | --- | --- |
| `POST` | `/api/v1/datasource/:id/nextcloud-event-connection` | Create one active connection; `201` once, then `409` on repeat. |
| `GET` | `/api/v1/datasource/:id/nextcloud-event-connection` | Read receipt, dispatch, and applied status; never returns a secret. |
| `POST` | `/api/v1/datasource/:id/nextcloud-event-connection/rotate` | Issue a new key; accept the previous key for two minutes; `200` returns the new secret once. |
| `POST` | `/api/v1/datasource/:id/nextcloud-event-connection/rebind` | After exact finalized source-key rotation, retain the connection, event key, inbox, and watermarks; `200` returns no secret. |
| `DELETE` | `/api/v1/datasource/:id/nextcloud-event-connection` | Revoke the active connection immediately; `204`. |

A successful pair or rotation returns `connection_id`, `key_id`, a one-time
`secret`, `nextcloud_instance_id`, `binding_id`, `status`, and `receiver_url`.
The receiver URL is the fixed absolute path
`/api/v1/integrations/nextcloud/events`; configure it under the public
WeKnora origin reachable from the Nextcloud server. Store the returned
connection ID, key ID, secret, instance ID, and binding ID in the Nextcloud
sender configuration. No management read can retrieve the secret again. If
the one-time response is lost, inspect status and rotate the key; repeating
Pair will not reveal an existing key. Rotation accepts the current and new
keys for two minutes measured by the receiver's clock, so the Nextcloud
sender can install the new one-time secret during that window. Another rotation
replaces the previous key; only the two most recent keys can be accepted.
After the window expires, the older key returns `401` and its sender may
pause. Keep the one-time response available while updating the sender. For
immediate credential termination, revoke the connection. A changed source is
reported as `source_changed` by GET; revoke and then pair again after resolving the
source. Revocation does not require a live Nextcloud call, so it remains
possible after credentials are cleared or the endpoint changes.

After both source-pairing sides report `finalized`, an administrator may send
`{"operation_id":"<exact source rotation UUID>"}` to the `rebind` route.
The live Nextcloud binding must be active and retain the pairing's exact
publication epoch, tenant, dedicated knowledge base, source, instance, binding
and normalized base URL. The local data source must also be active. WeKnora
verifies that
the stored old and new encrypted config fingerprints match the finalized
rotation, that only the source machine token and key ID changed, and that the
active pair uses the new fingerprint. A pending, aborted, unrelated or newer
rotation cannot rebind the connection. The request is idempotent for the same
rotation UUID; the response has `rebound:true` only on the first successful
rewrite and contains the unchanged `connection_id`, `key_id`, and decimal
received, dispatched and applied watermarks, plus the current dispatch state
and error code. It never returns the HMAC secret.
The Nextcloud event sender keeps its existing credentials.

Rebinding requires a quiescent event dispatch row: `idle`, or `blocked` solely
by `source_changed` after all previously dispatched hints were applied, with
no live lease or running source sync log. A queued, leased, retrying, partially
applied or differently blocked row returns `409`
`event_dispatch_manual_review_required`; inspect the exact task/log before
repairing it. Other identity mismatches return `409`
`source_changed_repair_required`. Generic source edits and emergency event-key
rotation remain separate operations. The operator command is
`python3 scripts/ops/local-source-rotation.py rebind --binding BINDING --pair-operation-id PAIR_UUID --operation-id ROTATION_UUID`.

Connection provisioning does not advance any event watermark.

## Background reconciliation and status

The dispatcher polls due connections every 5 seconds by default. Set
`WEKNORA_NEXTCLOUD_EVENT_DISPATCH_INTERVAL` to a Go duration from `1s` through
`1m` to tune this at process startup; an empty or invalid value uses `5s`.
For each connection it claims the current committed receipt watermark under a
database lease, then
queues an incremental source reconciliation. The connector validates the
complete current Nextcloud manifest and binding authorization before it
downloads touched files. Broad hints, an absent or expired changes cursor,
and the periodic content audit re-download all current files. An event hint
is never used directly as a delete instruction. A failed queue operation or
partial/failed/canceled sync remains retryable with exponential backoff.
Each sweep examines at most 32 due connections and runs them sequentially;
the poll interval alone is not an event-to-queue latency guarantee. The
Nextcloud sender cadence, network, database load, and queue admission also
affect that latency. Short intervals can increase reconciliation work under
bursty edits, so measure the pilot workload before claiming the PRD's P95 target.
Source configuration drift blocks dispatch; revoke blocks further claims and
cancels queued event tasks when they start. A running event task checks the
pinned connection and its running sync log before each knowledge item, and
rejects a fetched cursor from a different Nextcloud instance. A write already
in progress when an administrator revokes the connection can finish; revoke
does not provide an atomic cancellation barrier for that one write.

The administrator GET returns decimal-string watermarks with distinct meanings:

| Field | Meaning |
| --- | --- |
| `received_through_event_id` | Highest event ID committed in the inbox; the value returned by HTTP `202`. |
| `dispatched_through_event_id` | Highest receipt watermark for which the sync queue accepted a job. This does not prove that the job ran or succeeded. |
| `applied_through_event_id` | Highest event ID covered by WeKnora's complete publication proof. It can remain `"0"` while work is pending or proof is unavailable. |
| `backlog_count` | Inbox rows above the applied watermark, including rows already dispatched. |
| `undispatched_count` | Inbox rows above the dispatched watermark. |
| `dispatch_state` | `idle`, `leased`, `queued`, `retry`, or `blocked`. |
| `last_error_code` | Static failure category; contains no source URL or credential. |

The dispatch cursor and queue intent survive process restarts. The event task
creates a running sync log only when it starts. If the process dies before
enqueue, an expired lease with no log can be retried; a task that started is
reconciled by its exact log ID. An event task still marked running after 150
minutes moves to `blocked` with `sync_stale_manual_review`. An administrator
must investigate the source task and repair or re-pair the connection; the
dispatcher does not start a second task that might write concurrently.
Manual and scheduled Nextcloud syncs share the running-log admission slot.
If their queue enqueue result is uncertain, the running log keeps that slot;
if the queue did not actually accept the task, an administrator must verify
the queue and repair the log before another sync can start. These non-event
logs do not use the event dispatcher's 150-minute blocked transition.
`applied_through_event_id` does not advance. Source deletion still follows
the connector's two-complete-scan rule, so an absent file is not treated as
deleted from a single failed or partial manifest. A missing or malformed old
source cursor with prior imported knowledge blocks event dispatch for manual
review; the old inventory is required to find deletions. Manual and scheduled
syncs use the same baseline check. A live instance change fails the sync and
requires manual repair or re-pairing; until repaired, the dispatch state is
`retry` with `sync_not_successful` and may retry on its normal backoff.

Each event reconciliation validates and buffers the complete metadata manifest,
then emits one downloaded file at a time to the importer. Ordinary scoped
hints re-download only touched files. A separate content-audit deadline
defaults to 24 hours and forces a full content refresh even if other hinted
scans ran; opaque ETags and metadata cannot prove that an unhinted write did
not happen. The current file is held in memory as a byte slice (up to the
64 MiB download limit), and the manifest, old inventory, and new inventory
are proportional to file count. The source cursor advances only after the
whole scan and every emitted item have been processed successfully. An
interrupted scan therefore repeats its downloads. The 10,000-file / 100 GB
pilot target remains unaccepted; there is no resumable mid-scan checkpoint.
The five-second default dispatcher interval alone does not prove the PRD's
event-to-durable-job P95 target of 10 seconds.

## Request contract

The request body is JSON, at most 1 MiB and 200 hints. Event IDs and
`after_event_id` are decimal strings because database IDs may exceed the
integer precision of a JavaScript number. IDs are strictly increasing within
a batch; gaps are expected because Nextcloud allocates IDs across bindings.

```json
{
  "connection_id": "an-opaque-paired-connection-id",
  "nextcloud_instance_id": "example-instance",
  "binding_id": "example-binding",
  "after_event_id": "0",
  "events": [
    {"event_id": "41", "file_id": 123, "type": "upsert", "etag": "example-etag"}
  ]
}
```

Send one value each for `X-Nextcloud-Connection-Id`, `X-Nextcloud-Key-Id`,
`X-Nextcloud-Timestamp` (Unix seconds), `X-Nextcloud-Nonce` (32 lowercase hex
characters), and `X-Nextcloud-Signature` (64 lowercase hex characters).
The timestamp must be within five minutes of the receiver's clock. Each
connection/key/nonce can be consumed once. Retries must generate a fresh
nonce and signature.

The signature is lowercase hex HMAC-SHA256 using the paired, decrypted
connection secret as the key. Its message is these lines joined by `\n`,
without a trailing newline:

1. `nextcloud-event-hmac-sha256-v1`
2. `POST`
3. `/api/v1/integrations/nextcloud/events`
4. an empty line (queries are rejected)
5. lowercase hex SHA-256 of the exact request body bytes
6. timestamp header value
7. nonce header value
8. connection ID header value
9. key ID header value

The receiver reads the committed connection key under a database lock. A
previous key is accepted only while its explicit rotation window remains
valid. An expired or revoked key fails closed, including when another web
worker has a stale configuration cache.

`after_event_id` must equal the receiver's `received_id` for a new batch.
Replaying an older batch with a fresh nonce is accepted only if every event
ID and fingerprint already exist and none extends past the current watermark.
Otherwise the route returns `409` without advancing the watermark. The
nonce, all inbox rows, and the new watermark are one transaction, so a
database error cannot produce a false durable receipt. Reusing a nonce is
rejected even for an otherwise identical retry.

Hints remain non-authoritative. The dispatcher re-reads the current Nextcloud
manifest, authorization state, and file version before publication or deletion.
Outbox retention must not treat `202` or `dispatched_through_event_id` as an
applied checkpoint.

When parsing completes, WeKnora checks the candidate's original paired source
again with a signed `POST /bindings/{binding}/files/{file}/publication-check`.
The signed body fixes the imported instance ID, ETag, and path. Nextcloud
returns `204` only while the binding is active, the file remains readable in
that binding, its ETag and path still match, and the file is eligible for
publication. WeKnora compares the candidate, source config, and active pair
again under its database lock before setting the publication ETag, then probes
Nextcloud once more. A failed probe leaves or returns the candidate to staging
with an empty publication ETag. The source request has a five-second timeout.
These checks cannot form one transaction across the two services. A change
after the final probe is denied by the live source authorization check on
retrieval and is reconciled by later source scans.

There is also a visibility interval **between the local publication commit and
the final source probe**: the SQL publication ETag can briefly be present even
when Nextcloud has just withdrawn the file or advanced its ETag. If that probe
fails, WeKnora clears the exact candidate's marker and returns publication
failure; the repository race test injects both changes and verifies this
cleanup. The test deliberately observes the transient marker. A raw SQL or
vector consumer that bypasses the live source publication guard is therefore
unsafe. Human-facing reads and retrieval must reauthorize against Nextcloud
and compare the current source ETag before exposing source-derived content.
This two-probe protocol does not promise zero transient local visibility or
atomic revocation across the two services. A source change after an individual
read's live authorization but before that response is emitted is also outside
this protocol's atomicity guarantee.

## Signed applied status and Nextcloud retention

Nextcloud queries `GET /api/v1/integrations/nextcloud/events/status?connection_id=ID`
with an empty body. The raw query has exactly the `connection_id` key and the
connection's canonical ID; extra parameters, encoding changes, and redirects
are rejected. It uses the same connection ID, key ID, timestamp, nonce, and
signature headers as event delivery. The HMAC message has nine lines joined
by `\n`, without a final newline:

1. `nextcloud-event-status-hmac-sha256-v1`
2. `GET`
3. `/api/v1/integrations/nextcloud/events/status`
4. the exact raw query string
5. lowercase SHA-256 of the empty body
6. timestamp
7. nonce
8. connection ID
9. key ID

The signed status route checks the current key, live paired source, and replay
nonce before returning its connection, instance, binding, receipt, dispatch,
and applied watermarks. Nextcloud accepts the applied watermark only if the
connection, instance, and binding match its stored sender, the status is
active, the reported receipt covers its local receipt, and the applied ID is
monotonic and no greater than either receipt. A status failure or mismatch
blocks connected outbox pruning. The local sender status exposes its last
verified applied ID, check time, and status error without exposing the key.

The retention job still waits at least 30 days. For a connected binding, it
only removes a prefix through the verified applied ID after a valid status
check. A `202` receipt alone leaves the hints in the outbox. A missing or
failed status endpoint preserves them until the status is repaired; a later
full manifest reconciliation remains necessary after any already-expired
cursor or cross-system restore.

An owned, loopback-only synthetic LDAP dual-service exercise is available as
`scripts/ops/isolated-event-rebind-smoke.py --scratch OWNED_FIXTURE`. It
requires a bootstrapped fixture, creates e1 and e2 DAV files, runs both the
Nextcloud delivery and applied-status jobs, and requires both services to
report applied e1 before rotation and applied e2 after rebind. It verifies that
the old sender pauses and its receipt watermark does not advance while the
source config is stale. This is isolated integration evidence; it does not
replace real AD, production Team Folder, or load acceptance.
