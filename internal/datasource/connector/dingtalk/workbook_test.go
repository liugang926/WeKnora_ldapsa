package dingtalk

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestWorkbookSyncReadsCellsAndRetriesFailedRevision(t *testing.T) {
	const sheets = "/v1.0/doc/workbooks/book/sheets"
	var denied atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1.0/oauth2/accessToken" && (r.Method != http.MethodGet ||
			r.Header.Get("x-acs-dingtalk-access-token") != "token" || r.URL.Query().Get("operatorId") != "union/user") {
			http.Error(w, "invalid workbook request", http.StatusUnauthorized)
			return
		}
		var data string
		switch r.URL.Path {
		case "/v1.0/oauth2/accessToken":
			data = `{"accessToken":"token","expireIn":7200}`
		case "/v2.0/wiki/workspaces":
			data = `{"workspaces":[{"workspaceId":"w","rootNodeId":"root"}]}`
		case "/v2.0/wiki/nodes":
			data = `{"nodes":[{"nodeId":"book","name":"财务","type":"FILE","category":"ALIDOC",` +
				`"extension":"axls","modifiedTimestamp":2},{"nodeId":"ai","type":"FILE","category":"ALIDOC","extension":"able"}]}`
		case sheets:
			data = `{"value":[{"id":"s1","name":"预算"},{"id":"s2","name":"进度"}]}`
		case sheets + "/s1", sheets + "/s2":
			data = `{"lastNonEmptyRow":0,"lastNonEmptyColumn":1}`
		case sheets + "/s1/ranges/A1:B1":
			data = `{"displayValues":[["金额","¥5,000"]]}`
		case sheets + "/s2/ranges/A1:B1":
			if denied.Load() {
				http.Error(w, `{"code":"Forbidden"}`, http.StatusForbidden)
				return
			}
			data = `{"displayValues":[["完成率","45%"]]}`
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if strings.Contains(r.URL.Path, "/ranges/") && r.URL.Query().Get("select") != "displayValues" {
			t.Errorf("unexpected range select %q", r.URL.Query().Get("select"))
		}
		_, _ = fmt.Fprint(w, data)
	}))
	defer server.Close()
	connector := testConnector(testClient(server))
	ctx, invocation := BeginOriginalFetch(context.Background())
	children, err := connector.ListResources(ctx, testConfig(), "w")
	require.NoError(t, err)
	require.Len(t, children, 1)
	for _, selection := range []string{"w", children[0].ExternalID} {
		items, err := connector.FetchAll(ctx, testConfig(selection), []string{selection})
		require.NoError(t, err)
		require.Len(t, items, 1)
		require.Equal(t, "text/markdown", items[0].ContentType)
		require.Equal(t, "财务.md", items[0].FileName)
		require.Equal(t, "# 财务\n\n## 预算\nA1: 金额\nB1: ¥5,000\n"+
			"\n## 进度\nA1: 完成率\nB1: 45%\n", string(items[0].Content))
		_, err = VerifyOriginalFetchedArtifactForInvocation(invocation, &items[0])
		require.NoError(t, err)
	}
	old, err := encodeCursor(&cursorState{Version: cursorVersion, Resources: map[string]map[string]string{
		"w": {"book": "1"},
	}})
	require.NoError(t, err)
	denied.Store(true)
	items, next, err := connector.FetchIncremental(ctx, testConfig("w"), &types.SyncCursor{ConnectorCursor: old})
	var partial *datasource.PartialFetchError
	require.True(t, errors.As(err, &partial))
	require.Len(t, items, 1)
	require.Empty(t, items[0].Content)
	state, err := decodeCursor(next)
	require.NoError(t, err)
	require.Equal(t, "1", state.Resources["w"]["book"])
	denied.Store(false)
	items, next, err = connector.FetchIncremental(ctx, testConfig("w"), next)
	require.NoError(t, err)
	require.Len(t, items, 1)
	state, err = decodeCursor(next)
	require.NoError(t, err)
	require.Equal(t, "2", state.Resources["w"]["book"])
}
