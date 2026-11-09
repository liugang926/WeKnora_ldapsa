package service

import (
	"fmt"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
)

// TestCollectWikiImageRefs pins the deterministic image-reference collector:
// first-seen order, de-duplication across chunks, and the hard cap.
func TestCollectWikiImageRefs(t *testing.T) {
	chunks := []*types.Chunk{
		{Content: "Intro text.\n\n![cad_page_1.jpg](provider://local/cad_page_1.jpg)"},
		{Content: "![cad_page_1.jpg](provider://local/cad_page_1.jpg)\n\nmiddle text"},
		{Content: "plain tail chunk without images"},
	}
	for i := 0; i < 20; i++ {
		chunks = append(chunks, &types.Chunk{
			Content: fmt.Sprintf("![img%d](provider://local/img%d.jpg)", i, i),
		})
	}

	refs := collectWikiImageRefs(chunks)

	if len(refs) != maxWikiFallbackImages {
		t.Fatalf("expected cap of %d refs, got %d", maxWikiFallbackImages, len(refs))
	}
	if refs[0] != "![cad_page_1.jpg](provider://local/cad_page_1.jpg)" {
		t.Fatalf("first ref must keep first-seen order, got %q", refs[0])
	}
	dup := 0
	for _, r := range refs {
		if strings.Contains(r, "cad_page_1") {
			dup++
		}
	}
	if dup != 1 {
		t.Fatalf("duplicate ref collected %d times", dup)
	}
}

// TestEnsureWikiSummaryImages pins the deterministic image fallback for wiki
// summary pages: append when the LLM embedded nothing, leave alone when it did.
func TestEnsureWikiSummaryImages(t *testing.T) {
	refs := []string{
		"![cad_page_1.jpg](provider://local/cad_page_1.jpg)",
		"![cad_page_2.jpg](provider://local/cad_page_2.jpg)",
	}

	t.Run("appends section when body has no images", func(t *testing.T) {
		body := "# 摘要\n\n这份文档描述了底盘结构。"
		got := ensureWikiSummaryImages(body, refs, "zh-CN")
		if !strings.Contains(got, "## 相关图片") {
			t.Fatalf("missing fallback heading: %q", got)
		}
		for _, ref := range refs {
			if !strings.Contains(got, ref) {
				t.Fatalf("missing verbatim ref %q in %q", ref, got)
			}
		}
		if !strings.HasPrefix(got, body) {
			t.Fatalf("body must be preserved verbatim at the start")
		}
	})

	t.Run("leaves body unchanged when an image is embedded", func(t *testing.T) {
		body := "# Summary\n\n![kept](provider://local/kept.jpg)\n\nText."
		if got := ensureWikiSummaryImages(body, refs, "en"); got != body {
			t.Fatalf("body with an embedded image must be unchanged, got %q", got)
		}
	})

	t.Run("no refs means no change", func(t *testing.T) {
		body := "# Summary\n\nText only."
		if got := ensureWikiSummaryImages(body, nil, "en"); got != body {
			t.Fatalf("no refs must be a no-op, got %q", got)
		}
	})

	t.Run("idempotent on second application", func(t *testing.T) {
		body := "# 摘要\n\n文档说明。"
		once := ensureWikiSummaryImages(body, refs, "zh-CN")
		if twice := ensureWikiSummaryImages(once, refs, "zh-CN"); twice != once {
			t.Fatalf("second application must be a no-op")
		}
	})

	t.Run("localized headings", func(t *testing.T) {
		body := "text"
		if got := ensureWikiSummaryImages(body, refs, "ja"); !strings.Contains(got, "関連画像") {
			t.Fatalf("ja heading missing: %q", got)
		}
		if got := ensureWikiSummaryImages(body, refs, "en-US"); !strings.Contains(got, "Related Images") {
			t.Fatalf("en heading missing: %q", got)
		}
	})
}
