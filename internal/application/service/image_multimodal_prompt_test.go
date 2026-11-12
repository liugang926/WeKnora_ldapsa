package service

import (
	"context"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
)

func TestBuildVLMCaptionPrompt(t *testing.T) {
	t.Run("uses configured language and custom instructions", func(t *testing.T) {
		got := buildVLMCaptionPrompt(context.Background(), types.VLMConfig{
			DescriptionLanguage: "English",
			CustomInstructions:  "Focus on alarm codes.",
		})
		if !strings.Contains(got, "in English") || !strings.Contains(got, "Focus on alarm codes.") {
			t.Fatalf("unexpected prompt: %s", got)
		}
	})

	t.Run("defaults to context language", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), types.LanguageContextKey, "ko-KR")
		got := buildVLMCaptionPrompt(ctx, types.VLMConfig{})
		if !strings.Contains(got, "in Korean") {
			t.Fatalf("unexpected prompt: %s", got)
		}
	})
}

// TestVLMOCRPromptExcludesCustomInstructions pins the OCR-side half of the
// prompt contract: knowledge base custom instructions may shape *how images
// are described* (caption prompt) but must never reach the OCR prompt, where
// free-form business rules compete with the "No text content" output contract
// and produce poisoned image_ocr child chunks.
func TestVLMOCRPromptExcludesCustomInstructions(t *testing.T) {
	const marker = "UNIQUE_BUSINESS_MARKER_8f3a"
	cfg := types.VLMConfig{CustomInstructions: "For images without text output " + marker}

	for _, sourceType := range []string{"", "scanned_pdf", "vector_pdf"} {
		got := buildVLMOCRPrompt(sourceType, cfg)
		if strings.Contains(got, marker) {
			t.Fatalf("OCR prompt for source %q leaked custom instructions: %s", sourceType, got)
		}
		if !strings.Contains(got, "No text content") {
			t.Fatalf("OCR prompt for source %q lost the No text content contract: %s", sourceType, got)
		}
	}

	if got := buildVLMOCRPrompt("scanned_pdf", cfg); got != vlmOCRScannedPDFPrompt {
		t.Fatalf("scanned_pdf source must use the scanned-PDF prompt verbatim")
	}
	if got := buildVLMOCRPrompt("vector_pdf", cfg); got != vlmOCRVectorSheetPrompt {
		t.Fatalf("vector_pdf source must use the vector-sheet prompt verbatim")
	}
	if got := buildVLMOCRPrompt("regular", cfg); got != vlmOCRPrompt {
		t.Fatalf("regular source must use the default OCR prompt verbatim")
	}

	// The CAD prompt must ask for the title block (drawing name / number) —
	// that is the field the wiki page and the knowledge graph need.
	if got := buildVLMOCRPrompt("vector_pdf", cfg); !strings.Contains(got, "图纸名称") || !strings.Contains(got, "图号") {
		t.Fatalf("vector_pdf prompt lost the title-block extraction instructions: %s", got)
	}

	// The caption path keeps its custom instructions — only the OCR append was
	// removed.
	caption := buildVLMCaptionPrompt(context.Background(), cfg)
	if !strings.Contains(caption, marker) {
		t.Fatalf("caption prompt should still carry custom instructions: %s", caption)
	}
}

// TestVLMImageClassification pins the KB opt-in classification path: the
// system-owned classification prompt, tolerant label parsing, and the
// class -> dedicated OCR prompt mapping with plain-document fallback.
func TestVLMImageClassification(t *testing.T) {
	const marker = "UNIQUE_BUSINESS_MARKER_8f3a"

	t.Run("classification prompt is system-owned", func(t *testing.T) {
		// The classification prompt is compiled into the binary; the marker
		// check documents that KB custom instructions have no route into it.
		if strings.Contains(vlmImageClassifyPrompt, marker) {
			t.Fatalf("classification prompt must never embed KB custom instructions")
		}
		for _, label := range []string{"drawing_sheet", "product_image", "document"} {
			if !strings.Contains(vlmImageClassifyPrompt, label) {
				t.Fatalf("classification prompt must define the %q contract", label)
			}
		}
		if strings.Contains(vlmImageClassifyPrompt, "No text content") {
			t.Fatalf("classification prompt must not carry the OCR empty-output contract")
		}
	})

	t.Run("parse label tolerates wrappers", func(t *testing.T) {
		cases := map[string]string{
			"drawing_sheet":                 imageClassDrawingSheet,
			"  product_image \n":            imageClassProduct,
			"```document```":                imageClassDocument,
			"The answer is: drawing_sheet.": imageClassDrawingSheet,
			"\"product_image\"":             imageClassProduct,
			" unrelated reply":              "",
			"":                              "",
		}
		for reply, want := range cases {
			if got := parseImageClass(reply); got != want {
				t.Fatalf("parseImageClass(%q) = %q, want %q", reply, got, want)
			}
		}
	})

	t.Run("class selects dedicated prompt with fallback", func(t *testing.T) {
		if got := ocrPromptForClass(imageClassDrawingSheet, "scanned_pdf"); got != vlmOCRVectorSheetPrompt {
			t.Fatalf("drawing_sheet must use the CAD vector prompt")
		}
		if got := ocrPromptForClass(imageClassProduct, "pdf_text_layer"); got != vlmOCRProductImagePrompt {
			t.Fatalf("product_image must use the product prompt")
		}
		if got := ocrPromptForClass(imageClassDocument, "scanned_pdf"); got != vlmOCRScannedPDFPrompt {
			t.Fatalf("plain document must keep the scanned default")
		}
		if got := ocrPromptForClass("", "vector_pdf"); got != vlmOCRVectorSheetPrompt {
			t.Fatalf("unknown class must keep the source default")
		}
		for _, class := range []string{imageClassDrawingSheet, imageClassProduct} {
			if !strings.Contains(ocrPromptForClass(class, "scanned_pdf"), "No text content") {
				t.Fatalf("class %q prompt lost the No text content contract", class)
			}
		}
	})

	t.Run("dedicated prompts carry their field contracts", func(t *testing.T) {
		if got := vlmOCRVectorSheetPrompt; !strings.Contains(got, "图纸名称") || !strings.Contains(got, "图号") {
			t.Fatalf("drawing prompt lost the title-block instructions: %s", got)
		}
		if got := vlmOCRProductImagePrompt; !strings.Contains(got,
			"产品名称") ||
			!strings.Contains(got,
				"型号") ||
			!strings.Contains(got,
				"品牌") {

			t.Fatalf("product prompt lost the product-field instructions: %s", got)
		}
	})
}
