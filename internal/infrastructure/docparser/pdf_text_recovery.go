package docparser

import (
	"context"
	"strings"
	"unicode"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// pdfTextRecoveryReader keeps DocReader's page-aware PDF/OCR path as the
// default, but probes the in-process text extractor when the primary result
// contains suspiciously little searchable text. Some born-digital PDFs have
// usable Unicode text that DocReader's extractor silently drops.
type pdfTextRecoveryReader struct {
	primary   interfaces.DocReader
	alternate interfaces.DocReader
}

func (r *pdfTextRecoveryReader) Read(ctx context.Context, req *types.ReadRequest) (*types.ReadResult, error) {
	result, err := r.primary.Read(ctx, req)
	if err != nil || result == nil || req == nil || fileTypeOf(req) != "pdf" || len(req.FileContent) == 0 {
		return result, err
	}
	primaryChars := searchableRuneCount(result.MarkdownContent)
	if primaryChars >= 120 || r.alternate == nil {
		return result, nil
	}

	// The alternate is local-only and text-only. Its failure must not discard a
	// usable OCR result from DocReader (for example, a scanned PDF).
	alternateResult, alternateErr := r.alternate.Read(ctx, req)
	if alternateErr != nil || alternateResult == nil {
		return result, nil
	}
	alternateChars := searchableRuneCount(alternateResult.MarkdownContent)
	if alternateChars < 48 || alternateChars < primaryChars+32 || alternateChars < primaryChars*2 {
		return result, nil
	}

	// Preserve DocReader's image references and their markdown placements. If
	// there are no images, the richer text can replace the truncated text.
	if len(result.ImageRefs) > 0 {
		result.MarkdownContent = strings.TrimSpace(result.MarkdownContent) + "\n\n" +
			strings.TrimSpace(alternateResult.MarkdownContent)
	} else {
		result.MarkdownContent = alternateResult.MarkdownContent
	}
	if result.Metadata == nil {
		result.Metadata = make(map[string]string)
	}
	result.Metadata["pdf_text_recovered"] = AnydocEngineName
	logger.Infof(ctx, "[pdf] recovered incomplete text with anydoc: primary_chars=%d recovered_chars=%d",
		primaryChars, alternateChars)
	return result, nil
}

func searchableRuneCount(text string) int {
	count := 0
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			count++
		}
	}
	return count
}
