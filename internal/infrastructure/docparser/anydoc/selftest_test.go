package anydoc

import (
	"archive/zip"
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestSelfTestFixturesCarryBothFormats(t *testing.T) {
	if !bytes.HasPrefix(selfTestPDF(), []byte("%PDF-1.4")) {
		t.Fatal("PDF self-test fixture has no PDF signature")
	}
	docx := selfTestDOCX()
	if !bytes.HasPrefix(docx, []byte("PK")) {
		t.Fatal("DOCX self-test fixture has no ZIP signature")
	}
	archive, err := zip.NewReader(bytes.NewReader(docx), int64(len(docx)))
	if err != nil {
		t.Fatalf("open DOCX self-test fixture: %v", err)
	}
	for _, part := range archive.File {
		if part.Name != "word/document.xml" {
			continue
		}
		reader, err := part.Open()
		if err != nil {
			t.Fatalf("open DOCX document part: %v", err)
		}
		content, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("read DOCX document part: read=%v close=%v", readErr, closeErr)
		}
		if !bytes.Contains(content, []byte(selfTestDOCXText)) {
			t.Fatal("DOCX self-test document part does not contain the probe text")
		}
		return
	}
	t.Fatal("DOCX self-test fixture has no document part")
}

func TestSelfTestHonorsBackendAvailability(t *testing.T) {
	result, err := SelfTest()
	if !Available() {
		if err == nil || !strings.Contains(err.Error(), "anydoc unavailable") {
			t.Fatalf("SelfTest error = %v, want unavailable backend error", err)
		}
		return
	}
	if err != nil {
		t.Fatalf("SelfTest failed with a linked converter: %v", err)
	}
	if result.Version == "" || !result.Checks["pdf"] || !result.Checks["docx"] {
		t.Fatalf("incomplete self-test result: %+v", result)
	}
}
