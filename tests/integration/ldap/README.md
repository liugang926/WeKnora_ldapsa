# LDAP/AD protocol integration fixture

This fixture starts an isolated OpenLDAP 2.6 server and exercises the real
network path used by WeKnora's directory adapter. It is opt-in and is not part
of the default unit-test suite.

Covered behavior:

- service-account simple Bind followed by user search and user Bind;
- `sAMAccountName` and UPN authentication;
- RFC 2696 paging with a page size of one;
- certificate-verified LDAPS and StartTLS using an ephemeral private CA;
- rejection of an untrusted TLS certificate;
- ordered controller failover from a closed first endpoint;
- complete adapter synchronization, binary `objectGUID`/`objectSid` parsing,
  disabled-account flags, primary-group derivation, and nested groups.

Run from the repository root:

```bash
tests/integration/ldap/run.sh
```

The script generates short-lived certificates under `.generated/`, starts the
pinned multi-architecture containers, waits for the seeded directory to become
healthy, runs `go test -tags=integration`, and removes the containers and
volumes. Set `LDAP_IT_KEEP=1` to leave the fixture running for inspection.

The exposed debug endpoints are `ldap://localhost:11389` and
`ldaps://localhost:11636` by default. They can be changed with
`LDAP_IT_LDAP_PORT` and `LDAP_IT_LDAPS_PORT`.

## Scope and real AD acceptance

This is an OpenLDAP protocol fixture carrying a small, fixed AD-shaped schema
and binary attribute dataset. It verifies the LDAP wire behavior and the
adapter's AD parsing logic, but it is **not a real Microsoft Active Directory
domain**. No enterprise domain controller has been tested as part of this
change. AD-specific production acceptance must still follow
`scripts/ldap/acceptance-real-ad.sh` and the checklist in `docs/LDAP_AD.md`,
including certificate-chain/hostname checks, ranged group membership, actual
AD paging controls, primary groups, OU moves, nested groups, disabled users,
and multi-DC failure drills.

The image is pinned to an immutable legacy Bitnami digest solely for isolated
testing. It is not a recommended production LDAP deployment.
