package handler

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestNextcloudKBMetadataWhitelistsUnknownCountsAndBodyFallback(t *testing.T) {
	row := sourceKBStatsUnverified(map[string]interface{}{"id": "source", "name": "currently-authorized-root", "knowledge_count": 17, "generated_profile": "retired original body", "extract_config": "retired original prompts", "vector_store_id": "private inventory", "creator_name": "tenant unrelated"}).(map[string]interface{})
	require.Equal(t, "source", row["id"])
	require.Nil(t, row["knowledge_count"])
	require.Equal(t, "unverified", row["source_statistics_state"])
	for _, key := range []string{"generated_profile", "extract_config", "vector_store_id", "creator_name"} {
		require.NotContains(t, row, key)
	}
	fallback := sourceKBStatsUnverified(&types.KnowledgeBase{ID: "source", Name: "currently-authorized-root", Description: "retired original body", SummaryModelID: "private model"}).(map[string]interface{})
	require.Equal(t, "source", fallback["id"])
	require.NotContains(t, fallback, "description")
	require.NotContains(t, fallback, "summary_model_id")
}
