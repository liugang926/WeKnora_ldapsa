package utils

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

var defaultHTTPClient = NewSSRFSafeHTTPClient(SSRFSafeHTTPClientConfig{
	Timeout:      60 * time.Second,
	MaxRedirects: 10,
})

// DownloadBytes fetches the content at the given HTTP(S) URL and returns the
// raw bytes. It reuses a package-level http.Client with a 60-second timeout.
func DownloadBytes(url string) ([]byte, error) {
	return DownloadBytesContext(context.Background(), url)
}

// DownloadBytesContext binds remote image reads to the original worker's
// cancellation. A detached retry cannot complete a canceled source read.
func DownloadBytesContext(ctx context.Context, url string) ([]byte, error) {
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		return nil, fmt.Errorf("unsupported URL scheme: %s", url)
	}
	if err := ValidateURLForSSRF(url); err != nil {
		return nil, fmt.Errorf("URL rejected by SSRF policy: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := defaultHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP GET: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d for %s", resp.StatusCode, url)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	return data, nil
}
