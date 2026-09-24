package docparser

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/infrastructure/docparser/anydoc"
	"github.com/Tencent/WeKnora/internal/types"
)

func TestPDFTextRecoveryUsesRicherLocalText(t *testing.T) {
	primary := &stubDocReader{result: &types.ReadResult{MarkdownContent: "XH-CG-2026-01\nPDF"}}
	alternate := &stubDocReader{result: &types.ReadResult{MarkdownContent: strings.Repeat("中文采购审批正文。", 12)}}
	reader := &pdfTextRecoveryReader{primary: primary, alternate: alternate}
	req := &types.ReadRequest{FileContent: []byte("synthetic-pdf"), FileType: "pdf"}

	result, err := reader.Read(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if result.MarkdownContent != alternate.result.MarkdownContent {
		t.Fatalf("recovered markdown = %q", result.MarkdownContent)
	}
	if result.Metadata["pdf_text_recovered"] != AnydocEngineName {
		t.Fatalf("recovery metadata = %#v", result.Metadata)
	}
}

func TestPDFTextRecoveryKeepsDocReaderImages(t *testing.T) {
	primaryMarkdown := "![图](images/page-1.png)\nXH-CG-2026-01"
	primary := &stubDocReader{result: &types.ReadResult{
		MarkdownContent: primaryMarkdown,
		ImageRefs:       []types.ImageRef{{OriginalRef: "images/page-1.png"}},
	}}
	alternate := &stubDocReader{result: &types.ReadResult{MarkdownContent: strings.Repeat("中文审批正文。", 12)}}
	reader := &pdfTextRecoveryReader{primary: primary, alternate: alternate}

	result, err := reader.Read(context.Background(), &types.ReadRequest{
		FileContent: []byte("synthetic-pdf"), FileType: "pdf",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result.MarkdownContent, primaryMarkdown) ||
		!strings.Contains(result.MarkdownContent, alternate.result.MarkdownContent) || len(result.ImageRefs) != 1 {
		t.Fatalf("recovered PDF lost images or text: %#v", result)
	}
}

func TestPDFTextRecoveryLeavesHealthyAndScannedResults(t *testing.T) {
	for _, tc := range []struct {
		name        string
		primaryText string
		altText     string
		altErr      error
		wantAltCall bool
	}{
		{name: "healthy text", primaryText: strings.Repeat("中文正文", 40)},
		{name: "scanned OCR with no text layer", primaryText: "OCR 短句", altErr: errors.New("no text layer"), wantAltCall: true},
		{name: "alternate equally short", primaryText: "短句", altText: "另一短句", wantAltCall: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			primary := &stubDocReader{result: &types.ReadResult{MarkdownContent: tc.primaryText}}
			alternate := &stubDocReader{result: &types.ReadResult{MarkdownContent: tc.altText}, err: tc.altErr}
			reader := &pdfTextRecoveryReader{primary: primary, alternate: alternate}
			result, err := reader.Read(context.Background(), &types.ReadRequest{
				FileContent: []byte("synthetic-pdf"), FileType: "pdf",
			})
			if err != nil {
				t.Fatal(err)
			}
			if result.MarkdownContent != tc.primaryText || result.Metadata != nil {
				t.Fatalf("primary text unexpectedly replaced: %#v", result)
			}
			if (alternate.got != nil) != tc.wantAltCall {
				t.Fatalf("alternate called = %v, want %v", alternate.got != nil, tc.wantAltCall)
			}
		})
	}
}

func TestDefaultPDFReaderRecoversOnlyWhenAnydocLinked(t *testing.T) {
	remote := &stubDocReader{}
	reader, err := NewReader(context.Background(), "", "pdf", false, ReaderDeps{Remote: remote})
	if err != nil {
		t.Fatal(err)
	}
	if anydoc.Available() {
		if _, ok := reader.(*pdfTextRecoveryReader); !ok {
			t.Fatalf("default PDF reader = %T, want recovery wrapper", reader)
		}
	} else if reader != remote {
		t.Fatalf("default PDF reader = %T, want remote without anydoc", reader)
	}

	explicit, err := NewReader(context.Background(), BuiltinEngineName, "pdf", false, ReaderDeps{Remote: remote})
	if err != nil || explicit != remote {
		t.Fatalf("explicit builtin PDF reader = %T, err = %v", explicit, err)
	}
}
