package handler

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type directoryBrowseStub struct {
	interfaces.DirectoryRuntimeService
	guid, query   string
	limit, offset int
}

func (s *directoryBrowseStub) QueryGroupMembers(_ context.Context, guid, query string, limit, offset int) (*types.DirectoryGroupMembersResult, error) {
	s.guid, s.query, s.limit, s.offset = guid, query, limit, offset
	return &types.DirectoryGroupMembersResult{Items: []types.DirectoryGroupMember{}}, nil
}

func TestDirectoryGroupMembersHTTPPagination(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		query         string
		limit, offset int
	}{
		{"?q=alice&limit=50&offset=100", 50, 100},
		{"?limit=500&offset=-10", 100, 0},
		{"?limit=bad&offset=bad", 20, 0},
		{"", 20, 0},
	} {
		stub := &directoryBrowseStub{}
		h := &DirectoryHandler{runtime: stub}
		r := gin.New()
		h.RegisterAdminRoutes(r.Group("/system/admin"))
		response := httptest.NewRecorder()
		r.ServeHTTP(response, httptest.NewRequest("GET", "/system/admin/directory/groups/group-guid/members"+tc.query, nil))
		require.Equal(t, 200, response.Code)
		require.Equal(t, "group-guid", stub.guid)
		require.Equal(t, tc.limit, stub.limit)
		require.Equal(t, tc.offset, stub.offset)
		if tc.offset == 100 {
			require.Equal(t, "alice", stub.query)
		}
	}
}
