package session

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// SearchKnowledge runs the reranker before returning to the HTTP handler.
// Its score diagnostics must not turn an otherwise current source into a 403,
// nor bypass the broad/exact leases and output-boundary publication checks.
func TestRawSearchRankedSourceMetadataReturnsUnderExactLease(t *testing.T) {
	tests := []struct {
		name      string
		persisted map[string]string
		scores    map[string]string
	}{
		{"both_scores", nil, map[string]string{"base_score": "0.8500", "model_score": "0.9500"}},
		{"base_only", nil, map[string]string{"base_score": "-0.2500"}},
		{"model_only", nil, map[string]string{"model_score": "1.2e-3"}},
		{
			"persisted_score_unchanged",
			map[string]string{"base_score": "0.2500"},
			map[string]string{"base_score": "0.2500", "model_score": "0.9500"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := rawSearchLeaseDB(t)
			rawSearchCoverage(t, db)
			store := repository.NewNextcloudContentLeaseStore(db)
			row, result := rawSearchSource(t)
			persisted := row.GetMetadata()
			for key, value := range tt.persisted {
				persisted[key] = value
			}
			encoded, err := json.Marshal(persisted)
			require.NoError(t, err)
			row.Metadata = types.JSON(encoded)
			result.Metadata = maps.Clone(persisted)
			expectedResult := maps.Clone(persisted)
			for key, value := range tt.scores {
				expectedResult[key] = value
			}
			h, session, knowledge := rawSearchHandler(row, result, store)
			session.onSearch = func() {
				// These are the exact metadata keys added by PluginRerank.OnEvent.
				for key, value := range tt.scores {
					result.Metadata[key] = value
				}
			}
			w := rawSearchResponse(h, `{"query":"synthetic","knowledge_base_ids":["kb-1"]}`)
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Contains(t, w.Body.String(), result.Content)
			require.GreaterOrEqual(t, knowledge.checks.Load(), int32(3))
			require.Equal(t, persisted, row.GetMetadata())
			require.Equal(t, expectedResult, result.Metadata)
			assertRawSearchLeasesReleased(t, db, 2)
		})
	}
}

func TestRawSearchRankedSourceMetadataRejectsUntrustedChanges(t *testing.T) {
	tests := []struct {
		name   string
		change func(*types.Knowledge, *types.SearchResult)
	}{
		{"unknown_extra", func(_ *types.Knowledge, r *types.SearchResult) { r.Metadata["unexpected"] = "value" }},
		{"history_extra", func(_ *types.Knowledge, r *types.SearchResult) { r.Metadata["history_similarity"] = "0.9" }},
		{"changed_source", func(_ *types.Knowledge, r *types.SearchResult) { r.Metadata["nextcloud_file_id"] = "2" }},
		{"missing_source", func(_ *types.Knowledge, r *types.SearchResult) { delete(r.Metadata, "datasource_id") }},
		{"nil_metadata", func(_ *types.Knowledge, r *types.SearchResult) { r.Metadata = nil }},
		{"changed_channel", func(_ *types.Knowledge, r *types.SearchResult) { r.KnowledgeChannel = "api" }},
		{"changed_kb", func(_ *types.Knowledge, r *types.SearchResult) { r.KnowledgeBaseID = "kb-2" }},
		{"base_not_numeric", func(_ *types.Knowledge, r *types.SearchResult) { r.Metadata["base_score"] = "invalid" }},
		{"model_not_numeric", func(_ *types.Knowledge,
			r *types.SearchResult,
		) {
			r.Metadata["model_score"] = "invalid"
		}},
		{"base_empty", func(_ *types.Knowledge, r *types.SearchResult) { r.Metadata["base_score"] = "" }},
		{"base_nan", func(_ *types.Knowledge, r *types.SearchResult) { r.Metadata["base_score"] = "NaN" }},
		{"model_nan", func(_ *types.Knowledge, r *types.SearchResult) { r.Metadata["model_score"] = "NaN" }},
		{"base_positive_inf", func(_ *types.Knowledge, r *types.SearchResult) { r.Metadata["base_score"] = "+Inf" }},
		{"model_negative_inf", func(_ *types.Knowledge, r *types.SearchResult) { r.Metadata["model_score"] = "-Inf" }},
		{"model_overflow", func(_ *types.Knowledge, r *types.SearchResult) { r.Metadata["model_score"] = "1e1000" }},
		{"persisted_score_conflict", func(row *types.Knowledge, r *types.SearchResult) {
			metadata := row.GetMetadata()
			metadata["base_score"] = "0.2500"
			encoded, err := json.Marshal(metadata)
			require.NoError(t, err)
			row.Metadata = types.JSON(encoded)
			r.Metadata["base_score"] = "0.8500"
		}},
		{"persisted_score_missing", func(row *types.Knowledge, _ *types.SearchResult) {
			metadata := row.GetMetadata()
			metadata["model_score"] = "0.2500"
			encoded, err := json.Marshal(metadata)
			require.NoError(t, err)
			row.Metadata = types.JSON(encoded)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := rawSearchLeaseDB(t)
			rawSearchCoverage(t, db)
			store := repository.NewNextcloudContentLeaseStore(db)
			row, result := rawSearchSource(t)
			tt.change(row, result)
			persistedBefore, resultBefore := row.GetMetadata(), maps.Clone(result.Metadata)
			h, _, knowledge := rawSearchHandler(row, result, store)
			w := rawSearchResponse(h, `{"query":"synthetic","knowledge_base_ids":["kb-1"]}`)
			require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
			require.NotContains(t, w.Body.String(), result.Content)
			require.Zero(t, knowledge.checks.Load(), "changed identity must fail before publication checks")
			require.Equal(t, persistedBefore, row.GetMetadata())
			require.Equal(t, resultBefore, result.Metadata)
			assertRawSearchLeasesReleased(t, db, 1)
		})
	}
}

func TestRawSearchRankedSourceMetadataStillRejectsOutputBoundaryRevocation(t *testing.T) {
	db := rawSearchLeaseDB(t)
	rawSearchCoverage(t, db)
	store := repository.NewNextcloudContentLeaseStore(db)
	row, result := rawSearchSource(t)
	result.Metadata["base_score"] = "0.8500"
	result.Metadata["model_score"] = "0.9500"
	h, _, knowledge := rawSearchHandler(row, result, store)
	knowledge.onCheck = func(n int32) error {
		if n == 2 {
			return access.ErrNextcloudPublicationDenied
		}
		return nil
	}
	w := rawSearchResponse(h, `{"query":"synthetic","knowledge_base_ids":["kb-1"]}`)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), result.Content)
	require.Equal(t, int32(2), knowledge.checks.Load())
	assertRawSearchLeasesReleased(t, db, 2)
}

func TestRawSearchRankedSourceMetadataStillRejectsTransientPublicationFailure(t *testing.T) {
	db := rawSearchLeaseDB(t)
	rawSearchCoverage(t, db)
	store := repository.NewNextcloudContentLeaseStore(db)
	row, result := rawSearchSource(t)
	result.Metadata["base_score"] = "0.8500"
	result.Metadata["model_score"] = "0.9500"
	h, _, knowledge := rawSearchHandler(row, result, store)
	knowledge.onCheck = func(n int32) error {
		if n == 2 {
			return access.ErrNextcloudPublicationUnavailable
		}
		return nil
	}
	w := rawSearchResponse(h, `{"query":"synthetic","knowledge_base_ids":["kb-1"]}`)
	require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), result.Content)
	require.Equal(t, int32(2), knowledge.checks.Load())
	assertRawSearchLeasesReleased(t, db, 2)
}

func TestRawSearchRankedSourceMetadataFreshCheckCanDenyAfterAllowedHeader(t *testing.T) {
	db := rawSearchLeaseDB(t)
	rawSearchCoverage(t, db)
	store := repository.NewNextcloudContentLeaseStore(db)
	row, result := rawSearchSource(t)
	result.Metadata["base_score"] = "0.8500"
	result.Metadata["model_score"] = "0.9500"
	h, _, knowledge := rawSearchHandler(row, result, store)
	knowledge.onCheck = func(n int32) error {
		if n == 3 {
			return access.ErrNextcloudPublicationDenied
		}
		return nil
	}
	w := rawSearchResponse(h, `{"query":"synthetic","knowledge_base_ids":["kb-1"]}`)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), result.Content)
	require.Equal(t, int32(3), knowledge.checks.Load())
	assertRawSearchLeasesReleased(t, db, 2)
}

func TestRawSearchRankedSourceMetadataRejectsRetirementAndBlocksGC(t *testing.T) {
	db := rawSearchLeaseDB(t)
	rawSearchCoverage(t, db)
	store := repository.NewNextcloudContentLeaseStore(db)
	row, result := rawSearchSource(t)
	result.Metadata["base_score"] = "0.8500"
	result.Metadata["model_score"] = "0.9500"
	h, _, knowledge := rawSearchHandler(row, result, store)
	var retireErr, claimErr error
	knowledge.onCheck = func(n int32) error {
		if n != 2 {
			return nil
		}
		retireErr = store.RetireKnowledge(context.Background(), rawSearchScope(row))
		if retireErr != nil {
			return retireErr
		}
		_, claimErr = store.ClaimKnowledgeGC(context.Background(), rawSearchScope(row),
			rawSearchRetiredAt(t, db, row), time.Minute)
		return access.ErrNextcloudPublicationDenied
	}
	w := rawSearchResponse(h, `{"query":"synthetic","knowledge_base_ids":["kb-1"]}`)
	require.NoError(t, retireErr)
	require.ErrorIs(t, claimErr, repository.ErrNextcloudContentGCBusy)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), result.Content)
	require.Equal(t, int32(2), knowledge.checks.Load())
	assertRawSearchLeasesReleased(t, db, 2)
	// Once the response releases both leases, the retired generation may be
	// claimed for GC; the output fence did not permanently strand it.
	claim, err := store.ClaimKnowledgeGC(context.Background(), rawSearchScope(row),
		rawSearchRetiredAt(t, db, row), time.Minute)
	require.NoError(t, err)
	require.NoError(t, store.FinishGCClaim(context.Background(), claim, false))
}

func TestRawSearchOrdinaryLocalResultMetadataUnchanged(t *testing.T) {
	row := &types.Knowledge{ID: "local-doc", TenantID: 7, KnowledgeBaseID: "kb-1", Channel: "api"}
	result := &types.SearchResult{
		ID: "local-chunk", KnowledgeID: row.ID,
		KnowledgeBaseID: row.KnowledgeBaseID, KnowledgeChannel: row.Channel,
		Metadata: map[string]string{"local_key": "local_value", "base_score": "0.8500"},
		Content:  "synthetic local text",
	}
	h, _, knowledge := rawSearchHandler(row, result, nil)
	h.searchContentLeases = nil
	w := rawSearchResponse(h, `{"query":"synthetic","knowledge_base_ids":["kb-1"]}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), result.Content)
	require.Zero(t, knowledge.checks.Load())
	require.Equal(t, "local_value", result.Metadata["local_key"])
}

func TestRawSearchOrdinaryLocalResultCannotForgeSourceMetadata(t *testing.T) {
	row := &types.Knowledge{ID: "local-doc", TenantID: 7, KnowledgeBaseID: "kb-1", Channel: "api"}
	result := &types.SearchResult{
		ID: "local-chunk", KnowledgeID: row.ID,
		KnowledgeBaseID: row.KnowledgeBaseID, KnowledgeChannel: row.Channel,
		Metadata: map[string]string{"nextcloud_file_id": "1", "base_score": "0.8500"},
		Content:  "synthetic forged source text",
	}
	h, _, knowledge := rawSearchHandler(row, result, nil)
	h.searchContentLeases = nil
	w := rawSearchResponse(h, `{"query":"synthetic","knowledge_base_ids":["kb-1"]}`)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), result.Content)
	require.Zero(t, knowledge.checks.Load())
}

func TestRawSearchSourceMetadataMatcherPreservesNilAndStoredKeys(t *testing.T) {
	tests := []struct {
		name           string
		result, source map[string]string
		matches        bool
	}{
		{"nil_nil", nil, nil, true},
		{"nil_empty", nil, map[string]string{}, false},
		{"empty_nil", map[string]string{}, nil, false},
		{"empty_empty", map[string]string{}, map[string]string{}, true},
		{"score_without_source_metadata", map[string]string{"base_score": "0.5"}, nil, false},
		// Persisted fields retain the original exact-value contract. Numeric
		// validation applies only to additional pipeline score diagnostics.
		{
			"persisted_score_opaque_unchanged",
			map[string]string{"base_score": "stored-value"},
			map[string]string{"base_score": "stored-value"},
			true,
		},
		{
			"persisted_score_changed",
			map[string]string{"base_score": "0.5"},
			map[string]string{"base_score": "stored-value"},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultBefore, sourceBefore := maps.Clone(tt.result), maps.Clone(tt.source)
			require.Equal(t, tt.matches, rawSearchMetadataMatchesSource(tt.result, tt.source))
			require.Equal(t, resultBefore, tt.result)
			require.Equal(t, sourceBefore, tt.source)
		})
	}
}
