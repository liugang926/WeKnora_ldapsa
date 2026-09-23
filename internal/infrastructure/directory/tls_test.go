package directory

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildTLSConfigAlwaysVerifiesCertificateAndHostname(t *testing.T) {
	config, err := buildTLSConfig(
		Controller{URL: "ldaps://dc1.example.test:636", TLSMode: TLSModeLDAPS},
		TLSOptions{},
	)
	require.NoError(t, err)
	require.False(t, config.InsecureSkipVerify)
	require.Equal(t, "dc1.example.test", config.ServerName)
	require.Equal(t, uint16(tls.VersionTLS12), config.MinVersion)

	config, err = buildTLSConfig(
		Controller{URL: "ldap://10.0.0.1:389", TLSMode: TLSModeStartTLS, ServerName: "ad.example.test"},
		TLSOptions{MinVersion: tls.VersionTLS13},
	)
	require.NoError(t, err)
	require.Equal(t, "ad.example.test", config.ServerName)
	require.Equal(t, uint16(tls.VersionTLS13), config.MinVersion)
}

func TestBuildTLSConfigRejectsWeakTLSAndInvalidEnterpriseCA(t *testing.T) {
	_, err := buildTLSConfig(
		Controller{URL: "ldaps://dc.example.test", TLSMode: TLSModeLDAPS},
		TLSOptions{MinVersion: tls.VersionTLS11},
	)
	require.Error(t, err)

	_, err = buildTLSConfig(
		Controller{URL: "ldaps://dc.example.test", TLSMode: TLSModeLDAPS},
		TLSOptions{CAPEM: []byte("not a certificate")},
	)
	require.Error(t, err)
}

func TestBuildTLSConfigReadsCAFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(path, []byte("not a certificate"), 0o600))
	_, err := buildTLSConfig(
		Controller{URL: "ldaps://dc.example.test", TLSMode: TLSModeLDAPS},
		TLSOptions{CAFile: path},
	)
	require.Error(t, err)
}
