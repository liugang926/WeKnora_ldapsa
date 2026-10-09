package service

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type stubChunkForEmbed struct {
	interfaces.ChunkService
	chunks map[string]*types.Chunk
}

type stubKnowledgeForEmbed struct {
	interfaces.KnowledgeService
	docs map[string]*types.Knowledge
}

func (s *stubKnowledgeForEmbed) GetKnowledgeByIDOnly(_ context.Context, id string) (*types.Knowledge, error) {
	return s.docs[id], nil
}

func (s *stubChunkForEmbed) GetChunkByIDOnly(_ context.Context, id string) (*types.Chunk, error) {
	chunk, ok := s.chunks[id]
	if !ok {
		return nil, ErrChunkNotFound
	}
	return chunk, nil
}

func newEmbedChunkService(agent *types.CustomAgent, chunks map[string]*types.Chunk) *embedChannelService {
	docs := make(map[string]*types.Knowledge, len(chunks))
	for _, chunk := range chunks {
		if chunk.KnowledgeID == "" {
			chunk.KnowledgeID = "knowledge-" + chunk.ID
		}
		docs[chunk.KnowledgeID] = &types.Knowledge{
			ID: chunk.KnowledgeID, TenantID: chunk.TenantID, KnowledgeBaseID: chunk.KnowledgeBaseID,
		}
	}
	return &embedChannelService{
		agentService:     &stubAgentForEmbed{agent: agent},
		chunkService:     &stubChunkForEmbed{chunks: chunks},
		knowledgeService: &stubKnowledgeForEmbed{docs: docs},
	}
}

func TestChunkAllowedForEmbedAllowedKB(t *testing.T) {
	svc := newEmbedChunkService(
		&types.CustomAgent{
			Config: types.CustomAgentConfig{
				KBSelectionMode: "selected",
				KnowledgeBases:  []string{"kb-allowed"},
			},
		},
		map[string]*types.Chunk{
			"chunk-1": {ID: "chunk-1", TenantID: 5, KnowledgeBaseID: "kb-allowed"},
		},
	)
	ch := &types.EmbedChannel{ID: "ch-1", TenantID: 5, AgentID: "agent-1"}

	if !svc.chunkAllowedForEmbed(context.Background(), ch, &types.Chunk{TenantID: 5, KnowledgeBaseID: "kb-allowed"}) {
		t.Fatal("chunk in allowed KB should be permitted")
	}

	chunk, err := svc.EmbedChunk(context.Background(), ch, "chunk-1")
	if err != nil {
		t.Fatalf("EmbedChunk() = %v", err)
	}
	if chunk == nil || chunk.ID != "chunk-1" {
		t.Fatalf("unexpected chunk: %#v", chunk)
	}
}

func TestChunkAllowedForEmbedCrossTenantDenied(t *testing.T) {
	svc := newEmbedChunkService(
		&types.CustomAgent{
			Config: types.CustomAgentConfig{
				KBSelectionMode: "selected",
				KnowledgeBases:  []string{"kb-allowed"},
			},
		},
		map[string]*types.Chunk{
			"chunk-x": {ID: "chunk-x", TenantID: 999, KnowledgeBaseID: "kb-allowed"},
		},
	)
	ch := &types.EmbedChannel{ID: "ch-1", TenantID: 5, AgentID: "agent-1"}
	chunk := &types.Chunk{ID: "chunk-x", TenantID: 999, KnowledgeBaseID: "kb-allowed"}

	if svc.chunkAllowedForEmbed(context.Background(), ch, chunk) {
		t.Fatal("cross-tenant chunk must be denied before KB checks")
	}

	_, err := svc.EmbedChunk(context.Background(), ch, "chunk-x")
	if !errors.Is(err, ErrEmbedChunkForbidden) {
		t.Fatalf("EmbedChunk() = %v, want ErrEmbedChunkForbidden", err)
	}
}

func TestChunkAllowedForEmbedWrongKBDenied(t *testing.T) {
	svc := newEmbedChunkService(
		&types.CustomAgent{
			Config: types.CustomAgentConfig{
				KBSelectionMode: "selected",
				KnowledgeBases:  []string{"kb-allowed"},
			},
		},
		map[string]*types.Chunk{
			"chunk-other": {ID: "chunk-other", TenantID: 5, KnowledgeBaseID: "kb-other"},
		},
	)
	ch := &types.EmbedChannel{ID: "ch-1", TenantID: 5, AgentID: "agent-1"}
	chunk := &types.Chunk{ID: "chunk-other", TenantID: 5, KnowledgeBaseID: "kb-other"}

	if svc.chunkAllowedForEmbed(context.Background(), ch, chunk) {
		t.Fatal("chunk outside allowed KB list must be denied")
	}

	_, err := svc.EmbedChunk(context.Background(), ch, "chunk-other")
	if !errors.Is(err, ErrEmbedChunkForbidden) {
		t.Fatalf("EmbedChunk() = %v, want ErrEmbedChunkForbidden", err)
	}
}

func TestChunkAllowedForEmbedAllModePermitsInTenant(t *testing.T) {
	svc := newEmbedChunkService(
		&types.CustomAgent{
			Config: types.CustomAgentConfig{KBSelectionMode: "all"},
		},
		map[string]*types.Chunk{
			"chunk-all": {ID: "chunk-all", TenantID: 5, KnowledgeBaseID: "kb-any"},
		},
	)
	ch := &types.EmbedChannel{ID: "ch-1", TenantID: 5, AgentID: "agent-1"}

	if !svc.chunkAllowedForEmbed(context.Background(), ch, &types.Chunk{TenantID: 5, KnowledgeBaseID: "kb-any"}) {
		t.Fatal("all-mode agent should permit in-tenant chunks")
	}
}

func TestEmbedChunkRejectsNextcloudSourceForAnonymousChannel(t *testing.T) {
	svc := newEmbedChunkService(
		&types.CustomAgent{Config: types.CustomAgentConfig{
			KBSelectionMode: "selected", KnowledgeBases: []string{"kb-allowed"},
		}},
		map[string]*types.Chunk{"chunk-nextcloud": {
			ID: "chunk-nextcloud", TenantID: 5, KnowledgeBaseID: "kb-allowed", Content: "private text",
		}},
	)
	knowledge := svc.knowledgeService.(*stubKnowledgeForEmbed).docs["knowledge-chunk-nextcloud"]
	knowledge.Channel = types.ConnectorTypeNextcloud
	knowledge.Metadata = types.JSON(`{"datasource_id":"source-1","nextcloud_file_id":"42"}`)
	ch := &types.EmbedChannel{ID: "ch-1", TenantID: 5, AgentID: "agent-1"}
	if _, err := svc.EmbedChunk(context.Background(), ch, "chunk-nextcloud"); !errors.Is(err, ErrEmbedChunkForbidden) {
		t.Fatalf("Nextcloud chunk should be denied to embed visitor, got %v", err)
	}
}

func TestEmbedChunkRejectsMismatchedParentDocument(t *testing.T) {
	svc := newEmbedChunkService(
		&types.CustomAgent{Config: types.CustomAgentConfig{
			KBSelectionMode: "selected", KnowledgeBases: []string{"kb-allowed"},
		}},
		map[string]*types.Chunk{"chunk-1": {
			ID: "chunk-1", TenantID: 5, KnowledgeBaseID: "kb-allowed", Content: "text",
		}},
	)
	svc.knowledgeService.(*stubKnowledgeForEmbed).docs["knowledge-chunk-1"].KnowledgeBaseID = "kb-other"
	ch := &types.EmbedChannel{ID: "ch-1", TenantID: 5, AgentID: "agent-1"}
	if _, err := svc.EmbedChunk(context.Background(), ch, "chunk-1"); !errors.Is(err, ErrEmbedChunkForbidden) {
		t.Fatalf("mismatched parent should be denied, got %v", err)
	}
}
