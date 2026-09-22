package directory

import (
	"github.com/go-ldap/ldap/v3"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDirectoryDisplayNameFallback(t *testing.T) {
	for _, tc := range []struct{ display, name, cn, want string }{
		{" 张三 ", "name", "cn", "张三"},
		{" ", " 李四 ", "cn", "李四"},
		{"", "", " 王五 ", "王五"},
		{"", "", "", "alice"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			entry := testUserEntry("alice", 1107, 513)
			for _, attribute := range entry.Attributes {
				if attribute.Name == "displayName" {
					attribute.Values = []string{tc.display}
				}
			}
			entry.Attributes = append(entry.Attributes, &ldap.EntryAttribute{Name: "name", Values: []string{tc.name}}, &ldap.EntryAttribute{Name: "cn", Values: []string{tc.cn}})
			user, err := parseUserEntry(entry)
			require.NoError(t, err)
			require.Equal(t, tc.want, user.DisplayName)
			require.Equal(t, "alice", user.SAMAccountName)
		})
	}
	require.Contains(t, userAttributes, "name")
	require.Contains(t, userAttributes, "cn")
	require.Contains(t, groupAttributes, "cn")
	require.Contains(t, liveGroupIdentityAttributes, "cn")
}
