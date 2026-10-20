package agent

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

func TestDecodeDataURIBytes_Valid(t *testing.T) {
	raw := []byte{0xFF, 0xD8, 0xFF} // minimal bytes
	encoded := base64.StdEncoding.EncodeToString(raw)
	dataURI := "data:image/jpeg;base64," + encoded

	decoded, err := decodeDataURIBytes(dataURI)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(decoded) != len(raw) {
		t.Errorf("expected %d bytes, got %d", len(raw), len(decoded))
	}
}

func TestDecodeDataURIBytes_NoPaddingFallback(t *testing.T) {
	raw := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	// RawStdEncoding omits padding '='
	encoded := base64.RawStdEncoding.EncodeToString(raw)
	dataURI := "data:image/png;base64," + encoded

	decoded, err := decodeDataURIBytes(dataURI)
	if err != nil {
		t.Fatalf("unexpected error with padding fallback: %v", err)
	}
	if len(decoded) != len(raw) {
		t.Errorf("expected %d bytes, got %d", len(raw), len(decoded))
	}
}

func TestDecodeDataURIBytes_NoDataPrefix(t *testing.T) {
	_, err := decodeDataURIBytes("image/png;base64,AAAA")
	if err == nil {
		t.Fatal("expected error for missing data: prefix")
	}
	if !strings.Contains(err.Error(), "not a data URI") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestDecodeDataURIBytes_NoBase64Marker(t *testing.T) {
	_, err := decodeDataURIBytes("data:image/png,rawdata")
	if err == nil {
		t.Fatal("expected error for missing ;base64, marker")
	}
	if !strings.Contains(err.Error(), "unsupported") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestDecodeDataURIBytes_EmptyBase64(t *testing.T) {
	decoded, err := decodeDataURIBytes("data:image/png;base64,")
	if err != nil {
		t.Fatalf("unexpected error for empty base64: %v", err)
	}
	if len(decoded) != 0 {
		t.Errorf("expected 0 bytes for empty base64, got %d", len(decoded))
	}
}

func TestDescribeImages_WithoutInputBoundaryRefusesDescriber(t *testing.T) {
	engine := &AgentEngine{
		imageDescriber: func(_ context.Context, _ []byte, _ string) (string, error) {
			t.Fatal("an unadmitted image must not reach the VLM")
			return "", nil
		},
	}

	raw := []byte{0x89, 0x50, 0x4E, 0x47} // fake PNG header
	dataURI := "data:image/png;base64," + base64.StdEncoding.EncodeToString(raw)

	descriptions := engine.describeImages(context.Background(), []string{dataURI})
	if len(descriptions) != 0 || engine.lineageFailure() != ErrAgentSourceLineageDenied {
		t.Fatal("unadmitted image input must fail closed")
	}
}

func TestDescribeImages_UnadmittedInputNeverReachesFailingVLM(t *testing.T) {
	calls := 0
	engine := &AgentEngine{
		imageDescriber: func(_ context.Context, _ []byte, _ string) (string, error) {
			calls++
			return "", errors.New("VLM service unavailable")
		},
	}

	raw := []byte{0x89, 0x50}
	dataURI := "data:image/png;base64," + base64.StdEncoding.EncodeToString(raw)

	descriptions := engine.describeImages(context.Background(), []string{dataURI})
	if len(descriptions) != 0 || calls != 0 || engine.lineageFailure() != ErrAgentSourceLineageDenied {
		t.Fatal("unadmitted input must be rejected before the failing provider is invoked")
	}
}

func TestDescribeImages_InvalidDataURI(t *testing.T) {
	engine := &AgentEngine{
		imageDescriber: func(_ context.Context, _ []byte, _ string) (string, error) {
			t.Fatal("imageDescriber should not be called for invalid data URI")
			return "", nil
		},
	}

	descriptions := engine.describeImages(context.Background(), []string{"not-a-data-uri"})
	if len(descriptions) != 0 {
		t.Errorf("expected 0 descriptions for invalid URI, got %d", len(descriptions))
	}
}

func TestDescribeImages_ContextCancelled(t *testing.T) {
	callCount := 0
	engine := &AgentEngine{
		imageDescriber: func(_ context.Context, _ []byte, _ string) (string, error) {
			callCount++
			return "desc", nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	raw := []byte{0x89}
	dataURI := "data:image/png;base64," + base64.StdEncoding.EncodeToString(raw)

	descriptions := engine.describeImages(ctx, []string{dataURI, dataURI, dataURI})
	if callCount != 0 {
		t.Errorf("expected 0 VLM calls with cancelled context, got %d", callCount)
	}
	if len(descriptions) != 0 {
		t.Errorf("expected 0 descriptions, got %d", len(descriptions))
	}
}

func TestDescribeImages_NilDescriber(t *testing.T) {
	engine := &AgentEngine{
		imageDescriber: nil,
	}

	// Should not panic even with nil describer
	descriptions := engine.describeImages(context.Background(), []string{"data:image/png;base64,AAAA"})
	if len(descriptions) != 0 {
		t.Errorf("expected 0 descriptions with nil describer, got %d", len(descriptions))
	}
}

func TestDescribeImages_MultipleUnadmittedImagesNeverInvokeVLM(t *testing.T) {
	callIdx := 0
	engine := &AgentEngine{
		imageDescriber: func(_ context.Context, _ []byte, _ string) (string, error) {
			callIdx++
			if callIdx == 2 {
				return "", errors.New("fail on second")
			}
			return "ok", nil
		},
	}

	raw := []byte{0x89}
	dataURI := "data:image/png;base64," + base64.StdEncoding.EncodeToString(raw)

	descriptions := engine.describeImages(context.Background(), []string{dataURI, dataURI, dataURI})
	if len(descriptions) != 0 || callIdx != 0 || engine.lineageFailure() != ErrAgentSourceLineageDenied {
		t.Fatal("no unadmitted image may reach the VLM or publish a description")
	}
}
