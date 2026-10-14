# Signed Nextcloud file publication status

An installed Nextcloud event connection may query
`GET /api/v1/integrations/nextcloud/files/status` with the exact, unescaped
query order `connection_id=...&file_id=...&source_etag=...`. The file ID is a
positive canonical decimal string. The ETag is 1–256 ASCII letters, digits,
periods, underscores, colons or hyphens. The endpoint is registered outside
human authentication and accepts only a current or still-valid rotated event
connection key. It requires the connection's committed data source and source
pair to remain active.

Request headers are `X-Nextcloud-Connection-Id`, `X-Nextcloud-Key-Id`,
`X-Nextcloud-Timestamp`, `X-Nextcloud-Nonce`, and
`X-Nextcloud-Signature`. The signature is lower-case hex HMAC-SHA256 of these
newline-separated fields, with no trailing newline:

```
nextcloud-file-status-hmac-sha256-v1
GET
/api/v1/integrations/nextcloud/files/status
<exact query>
<SHA256 of empty body, lower-case hex>
<timestamp>
<nonce>
<connection ID>
<key ID>
```

The timestamp may differ by at most five minutes. Nonces share the durable
event connection nonce table and cannot be replayed, including across status
and event operations. Success is an exact JSON body with the connection ID,
Nextcloud instance, binding, tenant, knowledge base, data source, file ID,
queried ETag, knowledge state, published source ETag and ready time. The
`X-WeKnora-Status-Signature` header is lower-case hex HMAC-SHA256 of:

```
weknora-file-status-hmac-sha256-v1
<request signature, lower-case hex>
<SHA256 of exact response body bytes, lower-case hex>
<connection ID>
<key ID>
<nonce>
```

`ready` requires a `published` version row whose candidate is still present,
completed and enabled, with source identity metadata and publication ETag
matching the version row and requested ETag. A published older ETag or staging
is `updating`. A failed staged candidate for the requested ETag is `failed`.
Missing or inconsistent provenance is `unverified`. The response's
`qa_available` is always false: publication alone is not a per-user retrieval
grant. This feed reads persisted publication state. A source change after the
read is handled by Nextcloud's post-query ETag and access recheck and by
WeKnora's separate live retrieval guard.
