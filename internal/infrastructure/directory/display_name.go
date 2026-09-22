package directory

import (
	"github.com/go-ldap/ldap/v3"
	"strings"
)

// A display name is presentation data, never an identity/linking key.
func directoryDisplayName(entry *ldap.Entry) string {
	for _, attribute := range []string{"displayName", "name", "cn", "sAMAccountName"} {
		if value := strings.TrimSpace(entry.GetAttributeValue(attribute)); value != "" {
			return value
		}
	}
	return ""
}
