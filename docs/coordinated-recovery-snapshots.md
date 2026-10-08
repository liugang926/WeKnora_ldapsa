# Current recovery snapshots

The publication recovery CLI now supports `--mode=snapshot` with the existing
DB-owner transport and private file restrictions:

```sh
publication-recovery --mode=snapshot \
  --postgres-connection-file=/absolute/private/database.dsn \
  --plan-sha256=<persisted-plan-sha256> \
  --inventory=/absolute/private-external/original-inventory.json \
  --inventory-sha256=<independent-original-inventory-pin> \
  --output=/absolute/private/current-source-scope-state.json
```

This re-reads the persisted receipt, all original pairs, configuration fingerprints,
scopes, KB epochs, retired generations and source state. A retained receipt from
a partial restore that revived an old fence is refused. Reconciled sources also
perform fresh signed original pair ACKs around a complete double live manifest;
the result must still match their persisted reconciliation. Actual published
source lineage and relational indexed chunk IDs are included in the observation.
The command creates no user principal and does not grant personal reads or
global ingress. The external index backend still requires its own authoritative
inventory evidence. Unknown GC coverage remains blocked.

The CLI now reserves its exclusive safe output before connecting to the database.
An existing output file, directory or link refuses a mutating operation before
its transaction can run. Capture accepts Nextcloud's legitimate initial
publication epoch `0`; reconciliation still requires a strictly newer live epoch.

The companion Nextcloud coordinator binds the inventory into its cold checkpoint
and verifies both current state artifacts. Its isolated joint test used actual
maintenance CLI invocations and actual stock PostgreSQL backup/restore alongside
live Nextcloud and synthetic LDAP objectGUID/nested-share checks. Old history and
immutable IDs remained denied; a new ID used a fresh build lease and the actual
signed publication checker. The parser/index payloads were fixture rows, so the
test establishes this CLI/repository boundary and does not accept full deployed
app/index recovery or reopen global ingress.
