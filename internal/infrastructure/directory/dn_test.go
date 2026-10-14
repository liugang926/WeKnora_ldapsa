package directory

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNormalizeDNMatchesEquivalentLDAPRepresentations(t *testing.T) {
	for _, pair := range [][2]string{
		{`CN=Alice\, Doe,OU=Users,DC=example,DC=test`, `cn=Alice\2c Doe, ou=Users, dc=example, dc=test`},
		{`CN=Alice+UID=42,OU=Users,DC=example,DC=test`, `UID=42+CN=alice,OU=users,DC=EXAMPLE,DC=TEST`},
	} {
		require.Equal(t, normalizeDN(pair[0]), normalizeDN(pair[1]))
	}
}

func TestBuildSnapshotResolvesEquivalentMemberDN(t *testing.T) {
	user := User{
		ObjectGUID: "user-guid", SID: "S-1-5-21-1-2-3-1107",
		DN: `CN=Alice\, Doe,OU=Users,DC=example,DC=test`, Enabled: true, PrimaryGroupRID: 513,
	}
	group := parsedGroup{
		group: Group{
			ObjectGUID: "group-guid", SID: "S-1-5-21-1-2-3-513",
			DN: "CN=Users,OU=Groups,DC=example,DC=test",
		},
		members: []string{`cn=Alice\2c Doe, ou=Users, dc=example, dc=test`},
	}

	snapshot, err := buildSnapshot("corp-ad", "ldaps://dc.example.test", time.Now(), []User{user}, []parsedGroup{group})
	require.NoError(t, err)
	require.Empty(t, snapshot.UnresolvedMembers)
	require.Contains(t, snapshot.DirectMemberships, UserGroupMembership{
		UserGUID: user.ObjectGUID, GroupGUID: group.group.ObjectGUID, Source: MembershipDirect,
	})
	require.Contains(t, snapshot.DirectMemberships, UserGroupMembership{
		UserGUID: user.ObjectGUID, GroupGUID: group.group.ObjectGUID, Source: MembershipPrimary,
	})
}

func TestDirectoryMemberDNAnomaliesFailClosed(t *testing.T) {
	_, err := validateMemberValues([]string{"not a distinguished name"}, 100)
	require.ErrorIs(t, err, ErrInvalidDirectoryObject)
	_, err = validateMemberValues([]string{
		`CN=Alice\, Doe,OU=Users,DC=example,DC=test`,
		`CN=Alice\2c Doe, OU=Users, DC=example, DC=test`,
	}, 100)
	require.ErrorIs(t, err, ErrDuplicateDirectoryObject)

	_, err = buildSnapshot("corp-ad", "ldaps://dc.example.test", time.Now(), nil,
		[]parsedGroup{{
			group:   Group{ObjectGUID: "group-guid", DN: "CN=Users,DC=example,DC=test"},
			members: []string{"not a distinguished name"},
		}})
	require.ErrorIs(t, err, ErrInvalidDirectoryObject)
}
