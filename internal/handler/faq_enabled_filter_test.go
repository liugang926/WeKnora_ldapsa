package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
)

type faqListKnowledgeServiceStub struct {
	interfaces.KnowledgeService
	callCount int
	isEnabled *bool
}

func (s *faqListKnowledgeServiceStub) ListFAQEntries(
	_ context.Context,
	_ string,
	page *types.Pagination,
	_ []string,
	_ int64,
	_ string,
	_ string,
	_ string,
	isEnabled *bool,
) (*types.PageResult, error) {
	s.callCount++
	s.isEnabled = isEnabled
	return types.NewPageResult(0, page, []*types.FAQEntry{}), nil
}

func TestParseOptionalFAQEnabled(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name      string
		query     string
		want      bool
		wantValue bool
		wantError bool
	}{
		{name: "omitted"},
		{name: "enabled", query: "?is_enabled=true", want: true, wantValue: true},
		{name: "disabled", query: "?is_enabled=false", want: true},
		{name: "case insensitive", query: "?is_enabled=TRUE", want: true, wantValue: true},
		{name: "invalid", query: "?is_enabled=invalid", wantError: true},
		{name: "numeric", query: "?is_enabled=1", wantError: true},
		{name: "empty", query: "?is_enabled=", wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			context, _ := gin.CreateTestContext(httptest.NewRecorder())
			context.Request = httptest.NewRequest("GET", "/faq/entries"+tt.query, nil)

			value, err := parseOptionalFAQEnabled(context)
			if (err != nil) != tt.wantError {
				t.Fatalf("error = %v, wantError %v", err, tt.wantError)
			}
			if tt.wantError {
				return
			}
			if (value != nil) != tt.want {
				t.Fatalf("value presence = %v, want %v", value != nil, tt.want)
			}
			if value != nil && *value != tt.wantValue {
				t.Fatalf("value = %v, want %v", *value, tt.wantValue)
			}
		})
	}
}

func TestFAQListEntriesPassesEnabledFilter(t *testing.T) {
	h, _, _, _, _, _, ctx := faqActualHTTPFixture(t)
	for _, body := range []string{("{\"standard_question\":\"enabled original\",\"answers\":[\"enabled answer" +
		"\"],\"is_enabled\":true}"), ("{\"standard_question\":\"disabled original\",\"answers\":[\"disabled answe" +
		"r\"],\"is_enabled\":false}")} {
		w := serveFAQHTTP(ctx, h, http.MethodPost, "/knowledge-bases/kb/faq/entry", body)
		if w.Code != http.StatusOK {
			t.Fatalf("create status=%d: %s", w.Code, w.Body.String())
		}
	}
	for _, tc := range []struct{ query, total string }{{
		"",
		"2",
	}, {"?is_enabled=true", "1"}, {"?is_enabled=false", "1"}} {
		w := serveFAQHTTP(ctx, h, http.MethodGet, "/knowledge-bases/kb/faq/entries"+tc.query, "")
		if w.Code != http.StatusOK {
			t.Fatalf("status=%d: %s", w.Code, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), `"total":`+tc.total) {
			t.Fatalf("wrong total: %s", w.Body.String())
		}
	}
}

func TestFAQListEntriesRejectsInvalidEnabledFilter(t *testing.T) {
	gin.SetMode(gin.TestMode)

	service := &faqListKnowledgeServiceStub{}
	handler := &FAQHandler{knowledgeService: service}
	router := gin.New()
	router.Use(middleware.ErrorHandler())
	router.GET("/knowledge-bases/:id/faq/entries", handler.ListEntries)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/knowledge-bases/kb-1/faq/entries?is_enabled=invalid", nil)

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if service.callCount != 0 {
		t.Fatalf("service call count = %d, want 0", service.callCount)
	}
}
