package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type loadedSampleDeniedKnowledge struct {
	interfaces.KnowledgeService
	calls int
}

func (s *loadedSampleDeniedKnowledge) GetKnowledgeByIDOnly(context.Context, string) (*types.Knowledge, error) {
	s.calls++
	return nil, errors.New("ordinary knowledge read denied")
}

func TestSampleLoadedKnowledgeUsesOwnedCSVAndBoundedFixedQuery(t *testing.T) {
	ctx := context.Background()
	db := newPlainDuckDB(t)
	denied := &loadedSampleDeniedKnowledge{}
	tool := NewDataAnalysisTool(nil, denied, nil, nil, db, "bounded-loaded-sample")
	t.Cleanup(func() { tool.Cleanup(ctx) })
	knowledge := &types.Knowledge{ID: "sample-document", TenantID: 7, KnowledgeBaseID: "restricted", FileType: "csv"}
	rows := []string{"id,amount"}
	for i := 1; i <= 12; i++ {
		rows = append(rows, fmt.Sprintf("%d,%d", i, i*10))
	}
	schema, err := tool.LoadFromCSV(ctx, writeCSV(t, "source.csv", rows...), tool.TableName(knowledge))
	require.NoError(t, err)
	// A display schema returned to a caller is not the private loaded-table record.
	schema.TableName = "unrelated_global_table"
	schema.Columns = nil
	for _, limit := range []int{1, 10} {
		result, err := tool.SampleLoadedKnowledge(ctx, knowledge, limit)
		require.NoError(t, err)
		require.True(t, result.Success)
		require.Nil(t, result.SourceLineage, "sampling cannot manufacture a complete producer receipt")
		require.Len(t, result.Data["rows"].([]map[string]string), limit)
		require.Equal(t, fmt.Sprintf("SELECT * FROM dataset LIMIT %d", limit), result.Data["query"])
		require.Contains(t, result.Output, "record 1:")
	}
	for _, invalidLimit := range []int{-1, 0, 11, 1000000} {
		result, err := tool.SampleLoadedKnowledge(ctx, knowledge, invalidLimit)
		require.Error(t, err, "even a loaded table must obey the strict sampling limit")
		require.False(t, result.Success)
	}
	require.Zero(t, denied.calls, "internal fixed sampling must not re-enter ordinary read ACLs")
	args, err := json.Marshal(DataAnalysisInput{KnowledgeID: knowledge.ID, SQL: "SELECT * FROM dataset LIMIT 1"})
	require.NoError(t, err)
	result, err := tool.Execute(ctx, args)
	require.ErrorContains(t, err, "ordinary knowledge read denied")
	require.False(t, result.Success)
	require.Equal(t, 1, denied.calls, "interactive Execute still runs its original knowledge read boundary")
}

func TestSampleLoadedKnowledgeRejectsUnloadedAndInvalidRequests(t *testing.T) {
	ctx := context.Background()
	db := newPlainDuckDB(t)
	tool := NewDataAnalysisTool(nil, nil, nil, nil, db, "unloaded-sample")
	knowledge := &types.Knowledge{ID: "document"}
	for _, input := range []struct {
		name      string
		knowledge *types.Knowledge
		limit     int
	}{
		{name: "not-loaded", knowledge: knowledge, limit: 1},
		{name: "nil-knowledge", limit: 1},
		{name: "empty-id", knowledge: &types.Knowledge{}, limit: 1},
		{name: "zero-limit", knowledge: knowledge},
		{name: "negative-limit", knowledge: knowledge, limit: -1},
		{name: "over-limit", knowledge: knowledge, limit: 11},
		{name: "sql-shaped-id", knowledge: &types.Knowledge{ID: `document";SELECT * FROM secrets;--`}, limit: 1},
	} {
		t.Run(input.name, func(t *testing.T) {
			result, err := tool.SampleLoadedKnowledge(ctx, input.knowledge, input.limit)
			require.Error(t, err)
			require.False(t, result.Success)
			require.Empty(t, result.Output)
		})
	}
	var absent *DataAnalysisTool
	_, err := absent.SampleLoadedKnowledge(ctx, knowledge, 1)
	require.Error(t, err)
	_, err = (&DataAnalysisTool{}).SampleLoadedKnowledge(ctx, knowledge, 1)
	require.Error(t, err)
}

func TestSampleLoadedKnowledgeCannotBorrowGlobalOrOtherInstanceTable(t *testing.T) {
	ctx := context.Background()
	db := newPlainDuckDB(t)
	owner := NewDataAnalysisTool(nil, nil, nil, nil, db, "owner")
	other := NewDataAnalysisTool(nil, nil, nil, nil, db, "other")
	t.Cleanup(func() { owner.Cleanup(ctx) })
	knowledge := &types.Knowledge{ID: "owned"}
	path := writeCSV(t, "owned.csv", "id", "1")
	_, err := owner.LoadFromCSV(ctx, path, owner.TableName(knowledge))
	require.NoError(t, err)
	_, err = other.LoadFromTable(ctx, other.TableName(knowledge))
	require.NoError(t, err, "public schema inspection itself is not an owned successful load")
	_, err = other.SampleLoadedKnowledge(ctx, knowledge, 1)
	require.Error(t, err)
	for i := 0; i < 2; i++ {
		_, err = other.LoadFromCSV(ctx, path, other.TableName(knowledge))
		require.Error(t, err, "failed CREATE must never become successful ownership on a retry")
		_, err = other.SampleLoadedKnowledge(ctx, knowledge, 1)
		require.Error(t, err)
	}
	other.Cleanup(ctx)
	result, err := owner.SampleLoadedKnowledge(ctx, knowledge, 1)
	require.NoError(t, err, "another instance must not clean up the owner's table after failed CREATE")
	require.True(t, result.Success)
	_, err = owner.SampleLoadedKnowledge(ctx, &types.Knowledge{ID: "different"}, 1)
	require.Error(t, err, "a different document cannot use the loaded table")
}

func TestSampleLoadedKnowledgeRejectsMissingPhysicalTableAndEndedSession(t *testing.T) {
	ctx := context.Background()
	db := newPlainDuckDB(t)
	tool := NewDataAnalysisTool(nil, nil, nil, nil, db, "ended")
	knowledge := &types.Knowledge{ID: "document"}
	_, err := tool.LoadFromCSV(ctx, writeCSV(t, "source.csv", "id", "1"), tool.TableName(knowledge))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `DROP TABLE "k_document"`)
	require.NoError(t, err)
	result, err := tool.SampleLoadedKnowledge(ctx, knowledge, 1)
	require.Error(t, err)
	require.False(t, result.Success)
	tool.Cleanup(ctx)
	_, err = db.ExecContext(ctx, `CREATE TABLE "k_document" AS SELECT 'foreign' AS id`)
	require.NoError(t, err)
	_, err = tool.SampleLoadedKnowledge(ctx, knowledge, 1)
	require.Error(t, err, "session cleanup revokes loaded state even if a physical name is later reused")
}

func TestSampleLoadedKnowledgeCannotAdoptFailedCSVLoad(t *testing.T) {
	ctx := context.Background()
	db := newPlainDuckDB(t)
	tool := NewDataAnalysisTool(nil, nil, nil, nil, db, "failed")
	knowledge := &types.Knowledge{ID: "document"}
	_, err := tool.LoadFromCSV(ctx, "/missing-local-source.csv", tool.TableName(knowledge))
	require.Error(t, err)
	_, err = db.ExecContext(ctx, `CREATE TABLE "k_document" AS SELECT 'foreign' AS id`)
	require.NoError(t, err)
	_, err = tool.SampleLoadedKnowledge(ctx, knowledge, 1)
	require.Error(t, err, "a failed load cannot certify a subsequently appearing global table")
}

func TestSampleLoadedKnowledgeCannotCollapseDifferentDocumentIDs(t *testing.T) {
	ctx := context.Background()
	db := newPlainDuckDB(t)
	tool := NewDataAnalysisTool(nil, nil, nil, nil, db, "colliding-id")
	t.Cleanup(func() { tool.Cleanup(ctx) })
	original := &types.Knowledge{ID: "sample-document"}
	collision := &types.Knowledge{ID: "sample_document"}
	require.Equal(t, tool.TableName(original), tool.TableName(collision), "the legacy physical-name mapping collides")
	_, err := tool.LoadFromCSV(ctx, writeCSV(t, "source.csv", "id", "1"), tool.TableName(original))
	require.NoError(t, err)
	result, err := tool.SampleLoadedKnowledge(ctx, original, 1)
	require.NoError(t, err)
	require.True(t, result.Success)
	result, err = tool.SampleLoadedKnowledge(ctx, collision, 1)
	require.Error(t, err, "an unaccepted ID must not borrow an already-loaded physical table")
	require.False(t, result.Success)
	require.Empty(t, result.Output)
}
