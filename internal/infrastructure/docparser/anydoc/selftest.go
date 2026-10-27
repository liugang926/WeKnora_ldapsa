package anydoc

import (
	"archive/zip"
	"bytes"
	"fmt"
	"strings"
	"time"
)

const (
	// The self-test strings are intentionally plain ASCII so a
	// failed conversion cannot be confused with a font or encoding issue.
	selfTestPDFText  = "WeKnora PDF parser readiness"
	selfTestDOCXText = "WeKnora DOCX parser readiness"
)

// SelfTestResult records the exact converter build and successful probes.
type SelfTestResult struct {
	Version string
	Checks  map[string]bool
}

// SelfTest verifies the AnyDoc build with real PDF and DOCX conversions. It is
// deliberately independent of tenant configuration: a production image that
// advertises AnyDoc support must be able to parse both formats before it is
// allowed to become healthy.
func SelfTest() (SelfTestResult, error) {
	result := SelfTestResult{Version: Version(), Checks: map[string]bool{}}
	if !Available() {
		return result, fmt.Errorf("anydoc unavailable: %s", UnavailableReason())
	}
	for _, check := range []struct {
		name   string
		format string
		data   []byte
		want   string
	}{
		{"pdf", "pdf", selfTestPDF(), selfTestPDFText},
		{"docx", "docx", selfTestDOCX(), selfTestDOCXText},
	} {
		converted, err := Convert(check.data, Options{Format: check.format})
		if err != nil {
			return result, fmt.Errorf("anydoc %s self-test failed: %w", check.name, err)
		}
		if converted == nil || !strings.Contains(converted.Markdown, check.want) {
			return result, fmt.Errorf("anydoc %s self-test did not preserve the expected text", check.name)
		}
		result.Checks[check.name] = true
	}
	return result, nil
}

func selfTestPDF() []byte {
	content := "BT /F1 12 Tf 20 100 Td (" + selfTestPDFText + ") Tj ET\n"
	objects := []string{
		"<< /Type /Catalog /Pages 2 0 R >>",
		"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
		"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 320 200] /Contents 4 0 R " +
			"/Resources << /Font << /F1 5 0 R >> >> >>",
		fmt.Sprintf("<< /Length %d >>\nstream\n%sendstream", len(content), content),
		"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
	}
	var pdf bytes.Buffer
	offsets := make([]int, len(objects))
	pdf.WriteString("%PDF-1.4\n")
	for i, object := range objects {
		offsets[i] = pdf.Len()
		fmt.Fprintf(&pdf, "%d 0 obj\n%s\nendobj\n", i+1, object)
	}
	xref := pdf.Len()
	fmt.Fprintf(&pdf, "xref\n0 %d\n0000000000 65535 f \n", len(objects)+1)
	for _, offset := range offsets {
		fmt.Fprintf(&pdf, "%010d 00000 n \n", offset)
	}
	fmt.Fprintf(&pdf, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(objects)+1, xref)
	return pdf.Bytes()
}

func selfTestDOCX() []byte {
	parts := map[string]string{
		"[Content_Types].xml": `<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml"
    ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
  <Override PartName="/word/styles.xml"
    ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/>
</Types>`,
		"_rels/.rels": `<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1"
    Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument"
    Target="word/document.xml"/>
</Relationships>`,
		"word/styles.xml": `<?xml version="1.0" encoding="UTF-8"?>
<w:styles xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:style w:type="paragraph" w:default="1" w:styleId="Normal"><w:name w:val="Normal"/></w:style>
</w:styles>`,
		"word/_rels/document.xml.rels": `<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1"
    Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles"
    Target="styles.xml"/>
</Relationships>`,
		"word/document.xml": `<?xml version="1.0" encoding="UTF-8"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body><w:p><w:r><w:t>WeKnora DOCX parser readiness</w:t></w:r></w:p>
  <w:sectPr/></w:body>
</w:document>`,
	}
	var archiveBytes bytes.Buffer
	archive := zip.NewWriter(&archiveBytes)
	modified := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, name := range []string{
		"[Content_Types].xml",
		"_rels/.rels",
		"word/styles.xml",
		"word/_rels/document.xml.rels",
		"word/document.xml",
	} {
		header := &zip.FileHeader{Name: name, Method: zip.Deflate, Modified: modified}
		writer, err := archive.CreateHeader(header)
		if err != nil {
			panic("anydoc self-test DOCX header: " + err.Error())
		}
		if _, err := writer.Write([]byte(parts[name])); err != nil {
			panic("anydoc self-test DOCX content: " + err.Error())
		}
	}
	if err := archive.Close(); err != nil {
		panic("anydoc self-test DOCX archive: " + err.Error())
	}
	return archiveBytes.Bytes()
}
