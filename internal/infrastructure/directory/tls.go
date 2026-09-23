package directory

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"strings"
)

func buildTLSConfig(controller Controller, options TLSOptions) (*tls.Config, error) {
	parsed, err := url.Parse(strings.TrimSpace(controller.URL))
	if err != nil {
		return nil, fmt.Errorf("parse controller URL: %w", err)
	}
	serverName := strings.TrimSpace(controller.ServerName)
	if serverName == "" {
		serverName = parsed.Hostname()
	}
	if serverName == "" {
		return nil, fmt.Errorf("controller %q has no TLS server name", controller.URL)
	}

	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}

	caPEM := append([]byte(nil), options.CAPEM...)
	if path := strings.TrimSpace(options.CAFile); path != "" {
		fromFile, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil, fmt.Errorf("read enterprise CA file: %w", readErr)
		}
		if len(caPEM) > 0 {
			caPEM = append(caPEM, '\n')
		}
		caPEM = append(caPEM, fromFile...)
	}
	if len(caPEM) > 0 && !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("enterprise CA does not contain a valid PEM certificate")
	}

	minVersion := options.MinVersion
	if minVersion == 0 {
		minVersion = tls.VersionTLS12
	}
	if minVersion < tls.VersionTLS12 {
		return nil, fmt.Errorf("TLS minimum version must be TLS 1.2 or newer")
	}

	return &tls.Config{
		RootCAs:            roots,
		ServerName:         serverName,
		MinVersion:         minVersion,
		InsecureSkipVerify: false,
	}, nil
}
