package repository

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestSteerMaterialLegacyMessageRequestJSONAndDigestStayExact(t *testing.T) {
	legacy := ("{\"TenantID\":7,\"SessionID\":\"legacy-session\",\"MessageIDs\":[\"legacy-" +
		"message\"],\"AllSession\":false,\"CompletedOnly\":true,\"IncludeOriginalUs" +
		"erFields\":true,\"ExcludeRequestID\":\"legacy-request\"}")
	req := types.MessageMaterialReadRequest{
		TenantID:                  7,
		SessionID:                 "legacy-session",
		MessageIDs:                []string{"legacy-message"},
		CompletedOnly:             true,
		IncludeOriginalUserFields: true,
		ExcludeRequestID:          "legacy-request",
	}
	for _, empty := range [][]string{nil, {}} {
		req.ExcludeRequestIDs = empty
		encoded, err := json.Marshal(req)
		require.NoError(t, err)
		require.Equal(
			t,
			legacy,
			string(encoded),
			"empty forward-compatible fields must not alter known signed request bytes",
		)
		digest, err := KnowledgeInputDigest(req)
		require.NoError(t, err)
		oldDigest, err := KnowledgeInputDigest(json.RawMessage(legacy))
		require.NoError(t, err)
		require.Equal(t, oldDigest, digest)
	}
}

func TestSteerMaterialExcludedRequestPlanKeyCannotBorrowDifferentSelection(t *testing.T) {
	req := types.MessageMaterialReadRequest{
		TenantID:         7,
		SessionID:        "session",
		AllSession:       true,
		CompletedOnly:    true,
		ExcludeRequestID: "initial",
	}
	plan := &types.MessageMaterialReadPlan{Request: req}
	plan.Request.ExcludeRequestIDs = []string{"successor-a", "successor-b"}
	ctx := context.WithValue(context.Background(), messageMaterialPlanKey{}, []*types.MessageMaterialReadPlan{plan})
	require.Nil(t, findMessageMaterialReadPlan(ctx, req))
	req.ExcludeRequestIDs = []string{"successor-b", "successor-a", "successor-a"}
	require.Same(
		t,
		plan,
		findMessageMaterialReadPlan(ctx, req),
		"only the exact canonical exclusion set can borrow a plan",
	)
	req.ExcludeRequestIDs = []string{"successor-c", "successor-a"}
	require.Nil(t, findMessageMaterialReadPlan(ctx, req))
}
