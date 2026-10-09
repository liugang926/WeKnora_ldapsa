package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type protocolCreateDataSourceService struct {
	interfaces.DataSourceService
	err error
}

func (s protocolCreateDataSourceService) CreateDataSource(
	context.Context, *types.DataSource,
) (*types.DataSource, error) {
	return nil, s.err
}

func TestDataSourceProtocolErrorPreservesExistingPublicResponse(t *testing.T) {
	cause := errors.New("data source validation failed")
	typed := apperrors.NewProtocolError(cause, "Data source validation failed")
	wrappedCause := fmt.Errorf("create context: %w", typed)
	wrappedDisplay := apperrors.NewProtocolError(wrappedCause,
		"create context: "+apperrors.PublicMessage(typed))
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"typed_display", typed, "Data source validation failed"},
		{"ordinary_unchanged", cause, "data source validation failed"},
		// EOF is a real ordinary uppercase error, not a fixture-generated
		// custom uppercase diagnostic that evades ST1005.
		{"legacy_ordinary_uppercase", io.EOF, "EOF"},
		{"legacy_uppercase_context", fmt.Errorf("create context: %w", io.EOF), "create context: EOF"},
		{
			"outer_typed_layer_fallback_rule", wrappedCause,
			"create context: data source validation failed",
		},
		{"explicit_outer_legacy_display", wrappedDisplay, "create context: Data source validation failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			kb := &stubKBServiceForDS{getByID: func(context.Context, string) (*types.KnowledgeBase, error) {
				return &types.KnowledgeBase{ID: "kb1", TenantID: 7}, nil
			}}
			h := NewDataSourceHandler(protocolCreateDataSourceService{err: tc.err}, kb)
			router := gin.New()
			router.Use(func(c *gin.Context) {
				c.Set(types.TenantIDContextKey.String(), uint64(7))
				c.Next()
			})
			router.POST("/datasource", h.CreateDataSource)
			request := httptest.NewRequest(http.MethodPost, "/datasource",
				strings.NewReader(`{"knowledge_base_id":"kb1","type":"nextcloud"}`))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			require.Equal(t, http.StatusBadRequest, response.Code)
			var body map[string]any
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
			// This existing route has no numeric code envelope: preserve its
			// exact shape rather than adding a field or exposing the cause.
			require.Equal(t, map[string]any{"error": tc.want}, body)
		})
	}
	require.Equal(t, "data source validation failed", typed.Error())
	require.ErrorIs(t, typed, cause)
	require.ErrorIs(t, wrappedDisplay, typed)
	require.ErrorIs(t, wrappedDisplay, cause)
}

func TestMessageProtocolErrorPreservesExistingStatusCodeAndDisplay(t *testing.T) {
	cause := errors.New("history service unavailable")
	typed := apperrors.NewProtocolError(cause, "History service unavailable")
	wrappedCause := fmt.Errorf("history context: %w", typed)
	wrappedDisplay := apperrors.NewProtocolError(wrappedCause,
		"history context: "+apperrors.PublicMessage(typed))
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"typed_display", typed, "History service unavailable"},
		{"ordinary_unchanged", cause, "history service unavailable"},
		{"legacy_ordinary_uppercase", io.EOF, "EOF"},
		{"legacy_uppercase_context", fmt.Errorf("history context: %w", io.EOF), "history context: EOF"},
		{
			"outer_typed_layer_fallback_rule", wrappedCause,
			"history context: history service unavailable",
		},
		{"explicit_outer_legacy_display", wrappedDisplay, "history context: History service unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := &stubMessageService{
				getRecent: func(context.Context, string, int) ([]*types.Message, error) {
					return nil, tc.err
				},
			}
			response := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/messages/synthetic/load?limit=20", nil)
			newMessageTestRouter(service).ServeHTTP(response, request)
			require.Equal(t, http.StatusInternalServerError, response.Code)
			var body map[string]any
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
			require.Equal(t, map[string]any{
				"success": false,
				"error": map[string]any{
					"code": float64(apperrors.ErrInternalServer), "message": tc.want, "details": nil,
				},
			}, body)
		})
	}
	require.ErrorIs(t, wrappedDisplay, typed)
	require.ErrorIs(t, wrappedDisplay, cause)
}
