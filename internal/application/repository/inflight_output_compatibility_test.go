package repository

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func inflightOutputKnowledge(id, status string) *types.Knowledge {
	return &types.Knowledge{
		ID:              id,
		TenantID:        7,
		KnowledgeBaseID: "kb",
		Type:            types.KnowledgeTypeManual,
		Source:          "manual",
		Channel:         types.ChannelWeb,
		Title:           "Stable title",
		Description:     "processing description",
		ParseStatus:     status,
		FileName:        "stable.md",
		FileType:        "md",
		FileHash:        "source-hash",
		FilePath:        "files/stable.md",
		FolderPath:      "docs",
		Metadata:        types.JSON(`{"manual":{"content":"stable"}}`),
		CustomMetadata:  types.JSON(`{}`),
	}
}

func TestCompatibleCollectionSelectionAllowsOnlyInFlightOperationalChanges(t *testing.T) {
	parseStatus := types.ParseStatusProcessing
	leftKnowledge := inflightOutputKnowledge("knowledge-1", parseStatus)
	rightKnowledge := *leftKnowledge
	rightKnowledge.Description = "generated description"
	rightKnowledge.ParseStatus = types.ParseStatusCompleted
	rightKnowledge.PendingSubtasksCount = 2

	request := types.KnowledgeCollectionSelectionRequest{Mode: "list"}
	expected := &types.KnowledgeCollectionSelection{
		Request: request, Knowledge: []*types.Knowledge{leftKnowledge},
		Folders: &types.KnowledgeFolderTree{}, Total: 1, Page: 1, PageSize: 10,
	}
	actual := &types.KnowledgeCollectionSelection{
		Request: request, Knowledge: []*types.Knowledge{&rightKnowledge},
		Folders: &types.KnowledgeFolderTree{}, Total: 1, Page: 1, PageSize: 10,
	}
	require.NoError(t, compatibleCollectionSelection(expected, actual))

	actual.Knowledge[0].ParseStatus = types.ParseStatusCompleted
	require.NoError(t, compatibleCollectionSelection(expected, actual),
		"the stable source identity remains authoritative after completion")
}

func TestCompatibleCollectionSelectionRejectsStableOrStructuralChanges(t *testing.T) {
	baseKnowledge := inflightOutputKnowledge("knowledge-1", types.ParseStatusProcessing)
	baseRequest := types.KnowledgeCollectionSelectionRequest{Mode: "list"}
	base := func() *types.KnowledgeCollectionSelection {
		return &types.KnowledgeCollectionSelection{
			Request: baseRequest, Knowledge: []*types.Knowledge{baseKnowledge},
			Folders: &types.KnowledgeFolderTree{}, Total: 1, Page: 1, PageSize: 10,
		}
	}
	tests := []struct {
		name   string
		mutate func(*types.KnowledgeCollectionSelection)
	}{
		{"request", func(s *types.KnowledgeCollectionSelection) { s.Request.Mode = "search" }},
		{"membership", func(s *types.KnowledgeCollectionSelection) { s.Knowledge = nil; s.Total = 0 }},
		{"pagination", func(s *types.KnowledgeCollectionSelection) { s.Page = 2 }},
		{"folders", func(s *types.KnowledgeCollectionSelection) {
			s.Folders = &types.KnowledgeFolderTree{
				Folders: []*types.KnowledgeFolderNode{{Path: "changed", Name: "changed"}},
			}
		}},
		{"tenant", func(s *types.KnowledgeCollectionSelection) {
			s.Knowledge[0] = inflightOutputKnowledge("knowledge-1", types.ParseStatusProcessing)
			s.Knowledge[0].TenantID = 8
		}},
		{"knowledge base", func(s *types.KnowledgeCollectionSelection) {
			s.Knowledge[0] = inflightOutputKnowledge("knowledge-1", types.ParseStatusProcessing)
			s.Knowledge[0].KnowledgeBaseID = "other"
		}},
		{"identity", func(s *types.KnowledgeCollectionSelection) {
			s.Knowledge[0] = inflightOutputKnowledge("knowledge-2", types.ParseStatusProcessing)
		}},
		{"title", func(s *types.KnowledgeCollectionSelection) {
			s.Knowledge[0] = inflightOutputKnowledge("knowledge-1", types.ParseStatusProcessing)
			s.Knowledge[0].Title = "changed"
		}},
		{"file name", func(s *types.KnowledgeCollectionSelection) {
			s.Knowledge[0] = inflightOutputKnowledge("knowledge-1", types.ParseStatusProcessing)
			s.Knowledge[0].FileName = "changed.md"
		}},
		{"folder path", func(s *types.KnowledgeCollectionSelection) {
			s.Knowledge[0] = inflightOutputKnowledge("knowledge-1", types.ParseStatusProcessing)
			s.Knowledge[0].FolderPath = "other"
		}},
		{"source root", func(s *types.KnowledgeCollectionSelection) {
			s.Knowledge[0] = inflightOutputKnowledge("knowledge-1", types.ParseStatusProcessing)
			s.Knowledge[0].FileHash = "changed"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := base()
			test.mutate(actual)
			require.ErrorIs(t, compatibleCollectionSelection(base(), actual), ErrKnowledgeInputChanged)
		})
	}
}

func TestCompatibleCollectionSelectionRejectsStableOperationalChanges(t *testing.T) {
	left := inflightOutputKnowledge("knowledge-1", types.ParseStatusCompleted)
	right := *left
	right.Description = "changed after completion"
	request := types.KnowledgeCollectionSelectionRequest{Mode: "list"}
	expected := &types.KnowledgeCollectionSelection{
		Request: request, Knowledge: []*types.Knowledge{left}, Total: 1, Page: 1, PageSize: 10,
	}
	actual := &types.KnowledgeCollectionSelection{
		Request: request, Knowledge: []*types.Knowledge{&right}, Total: 1, Page: 1, PageSize: 10,
	}
	require.ErrorIs(t, compatibleCollectionSelection(expected, actual), ErrKnowledgeInputChanged)
}

func tagOutputFixtureSnapshot() *types.TagOutputInputSnapshot {
	knowledge := inflightOutputKnowledge("knowledge-1", types.ParseStatusProcessing)
	return &types.TagOutputInputSnapshot{
		Request:       types.TagOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb"},
		ScopeRevision: 9,
		InFlight:      true,
		Tags:          []*types.KnowledgeTag{{ID: "tag-1", TenantID: 7, KnowledgeBaseID: "kb", Name: "stable"}},
		TagProofs:     []*types.KnowledgeCollectionAuxiliaryProof{{Kind: "tag-name", ID: "tag-1"}},
		Knowledge: []*types.KnowledgeCollectionCandidate{{
			Knowledge: knowledge,
			Proof:     &types.KnowledgeBuildInputProof{Version: 1},
		}},
		Chunks: []*types.TagOutputChunkCandidate{{
			Chunk: &types.Chunk{
				ID: "chunk-1", TenantID: 7, KnowledgeBaseID: "kb",
				KnowledgeID: "knowledge-1", Content: "processing content",
			},
			Revision: 3,
			Digest:   "digest-3",
		}},
		Auxiliary: &types.KnowledgeCollectionAuxiliary{
			KnowledgeBaseNames: map[string]string{"kb": "Stable KB"},
		},
	}
}

func TestCompatibleTagOutputInputsAllowsOnlyInFlightOperationalChanges(t *testing.T) {
	expected := tagOutputFixtureSnapshot()
	actual, err := cloneTagOutput(expected)
	require.NoError(t, err)
	actual.ScopeRevision++
	actual.InFlight = false
	actual.Knowledge[0].Knowledge.Description = "completed description"
	actual.Knowledge[0].Knowledge.ParseStatus = types.ParseStatusCompleted
	actual.Chunks[0].Chunk.Content = "completed content"
	actual.Chunks[0].Revision++
	actual.Chunks[0].Digest = "digest-4"

	require.True(t, compatibleTagOutputInputs(expected, actual))
}

func TestCompatibleTagOutputInputsRejectsUnrelatedOrStableChanges(t *testing.T) {
	base := func() *types.TagOutputInputSnapshot {
		snapshot := tagOutputFixtureSnapshot()
		stable := inflightOutputKnowledge("knowledge-2", types.ParseStatusCompleted)
		snapshot.Knowledge = append(snapshot.Knowledge, &types.KnowledgeCollectionCandidate{
			Knowledge: stable,
			Proof:     &types.KnowledgeBuildInputProof{Version: 1},
		})
		snapshot.Chunks = append(snapshot.Chunks, &types.TagOutputChunkCandidate{
			Chunk: &types.Chunk{
				ID: "chunk-2", TenantID: 7, KnowledgeBaseID: "kb",
				KnowledgeID: stable.ID, Content: "stable content",
			},
			Revision: 4,
			Digest:   "digest-4",
		})
		return snapshot
	}
	tests := []struct {
		name   string
		mutate func(*types.TagOutputInputSnapshot)
	}{
		{"request", func(s *types.TagOutputInputSnapshot) { s.Request.TenantID = 8 }},
		{"tags", func(s *types.TagOutputInputSnapshot) { s.Tags[0].Name = "changed" }},
		{"tag proofs", func(s *types.TagOutputInputSnapshot) { s.TagProofs[0].Digest = "changed" }},
		{"membership", func(s *types.TagOutputInputSnapshot) { s.Knowledge = nil }},
		{"tenant", func(s *types.TagOutputInputSnapshot) { s.Knowledge[0].Knowledge.TenantID = 8 }},
		{"knowledge base", func(s *types.TagOutputInputSnapshot) {
			s.Knowledge[0].Knowledge.KnowledgeBaseID = "other"
		}},
		{"identity", func(s *types.TagOutputInputSnapshot) { s.Knowledge[0].Knowledge.ID = "other" }},
		{"title", func(s *types.TagOutputInputSnapshot) { s.Knowledge[0].Knowledge.Title = "changed" }},
		{"folder path", func(s *types.TagOutputInputSnapshot) { s.Knowledge[0].Knowledge.FolderPath = "other" }},
		{"source root", func(s *types.TagOutputInputSnapshot) { s.Knowledge[0].Knowledge.FileHash = "changed" }},
		{"stable knowledge", func(s *types.TagOutputInputSnapshot) {
			s.Knowledge[1].Knowledge.Description = "changed"
		}},
		{"stable chunk", func(s *types.TagOutputInputSnapshot) {
			s.Chunks[1].Chunk.Content = "changed content"
		}},
		{"chunk identity", func(s *types.TagOutputInputSnapshot) { s.Chunks[0].Chunk.ID = "other" }},
		{"auxiliary", func(s *types.TagOutputInputSnapshot) {
			s.Auxiliary.KnowledgeBaseNames["kb"] = "Changed KB"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expected := base()
			actual, err := cloneTagOutput(expected)
			require.NoError(t, err)
			test.mutate(actual)
			require.False(t, compatibleTagOutputInputs(expected, actual))
		})
	}
}
