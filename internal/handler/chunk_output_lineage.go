package handler

import (
	"context"
	"net/http"
	"sort"

	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

// ConfigureChunkHandlerOutputLineage configures the chunk handler output lineage boundary.
func ConfigureChunkHandlerOutputLineage(h *ChunkHandler, authorizer *service.ChunkOutputAuthorizer) {
	if h != nil {
		h.outputAuthorizer = authorizer
	}
}

func (h *ChunkHandler) getOriginalChunkOutput(c *gin.Context) {
	ctx := c.Request.Context()
	id := c.Param("id")
	if id == "" {
		_ = c.Error(errors.NewBadRequestError("Chunk ID cannot be empty"))
		return
	}
	if h.outputAuthorizer == nil {
		_ = c.Error(knowledgeOutputHTTPError(types.ErrSourceLineageUnknown))
		return
	}
	hint, err := h.service.GetChunkByIDOnly(ctx, id)
	if err != nil || hint == nil {
		_ = c.Error(errors.NewNotFoundError("Chunk not found"))
		return
	}
	knowledge, err := h.loadPublishedKnowledge(ctx, hint.KnowledgeID)
	if err != nil {
		_ = c.Error(err)
		return
	}
	if knowledge.ID != hint.KnowledgeID || knowledge.KnowledgeBaseID != hint.KnowledgeBaseID ||
		knowledge.TenantID != hint.TenantID {
		_ = c.Error(knowledgeOutputHTTPError(types.ErrSourceLineageUnknown))
		return
	}
	leases, err := h.beginNextcloudRead(ctx, c, knowledge)
	if err != nil {
		_ = c.Error(err)
		return
	}
	defer leases.Close()
	ctx = leases.Context(ctx)
	guard, err := h.outputAuthorizer.Begin(
		ctx,
		types.ChunkOutputReadRequest{
			TenantID:        knowledge.TenantID,
			KnowledgeBaseID: knowledge.KnowledgeBaseID,
			KnowledgeID:     knowledge.ID,
			ChunkIDs:        []string{id},
		},
	)
	if err != nil {
		_ = c.Error(knowledgeOutputHTTPError(err))
		return
	}
	snapshot, err := guard.Snapshot()
	if err != nil || len(snapshot.Chunks) != 1 {
		_ = c.Error(knowledgeOutputHTTPError(types.ErrSourceLineageUnknown))
		return
	}
	if err := leases.Verify(); err != nil {
		_ = c.Error(nextcloudHTTPLeaseError(err))
		return
	}
	defer guardNextcloudResponseWrites(c, leases, func(checkCtx context.Context) error {
		if err := guard.Check(checkCtx, snapshot); err != nil {
			return knowledgeOutputHTTPError(err)
		}
		_, err := h.loadPublishedKnowledge(checkCtx, knowledge.ID)
		return err
	})()
	c.JSON(http.StatusOK, gin.H{"success": true, "data": snapshot.Chunks[0]})
}

func (h *ChunkHandler) listOriginalChunkOutputs(c *gin.Context) {
	ctx := c.Request.Context()
	id := c.Param("knowledge_id")
	if id == "" {
		_ = c.Error(errors.NewBadRequestError("Knowledge ID cannot be empty"))
		return
	}
	if h.outputAuthorizer == nil {
		_ = c.Error(knowledgeOutputHTTPError(types.ErrSourceLineageUnknown))
		return
	}
	knowledge, err := h.loadPublishedKnowledge(ctx, id)
	if err != nil {
		_ = c.Error(err)
		return
	}
	leases, err := h.beginNextcloudRead(ctx, c, knowledge)
	if err != nil {
		_ = c.Error(err)
		return
	}
	defer leases.Close()
	ctx = leases.Context(ctx)
	var pagination types.Pagination
	if err := c.ShouldBindQuery(&pagination); err != nil {
		_ = c.Error(errors.NewBadRequestError("Invalid pagination"))
		return
	}
	if pagination.Page < 1 {
		pagination.Page = 1
	}
	if pagination.PageSize < 1 {
		pagination.PageSize = 10
	}
	if pagination.PageSize > 100 {
		pagination.PageSize = 100
	}
	guard, err := h.outputAuthorizer.Begin(
		ctx,
		types.ChunkOutputReadRequest{
			TenantID:        knowledge.TenantID,
			KnowledgeBaseID: knowledge.KnowledgeBaseID,
			KnowledgeID:     knowledge.ID,
			AllChunks:       true,
		},
	)
	if err != nil {
		_ = c.Error(knowledgeOutputHTTPError(err))
		return
	}
	snapshot, err := guard.Snapshot()
	if err != nil {
		_ = c.Error(knowledgeOutputHTTPError(err))
		return
	}
	// Account and authorize every original candidate before type filtering,
	// sorting, page selection or totals. A nonmatching legacy row stays unknown.
	wanted := c.QueryArray("chunk_type")
	if len(wanted) == 0 {
		wanted = []string{types.ChunkTypeText}
	}
	typesSet := map[string]bool{}
	for _, kind := range wanted {
		typesSet[kind] = true
	}
	rows := []*types.Chunk{}
	for _, chunk := range snapshot.Chunks {
		if typesSet[chunk.ChunkType] {
			rows = append(rows, chunk)
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].ChunkIndex != rows[j].ChunkIndex {
			return rows[i].ChunkIndex < rows[j].ChunkIndex
		}
		return rows[i].ID < rows[j].ID
	})
	total := len(rows)
	// Multiplication only follows a bounded comparison, avoiding overflowing an
	// attacker-supplied page integer into a negative/small slice index.
	start := total
	if pagination.Page-1 <= total/pagination.PageSize {
		start = (pagination.Page - 1) * pagination.PageSize
		if start > total {
			start = total
		}
	}
	end := start + pagination.PageSize
	if end > total {
		end = total
	}
	page := rows[start:end]
	if err := guard.Check(ctx, snapshot); err != nil {
		_ = c.Error(knowledgeOutputHTTPError(err))
		return
	}
	if err := leases.Verify(); err != nil {
		_ = c.Error(nextcloudHTTPLeaseError(err))
		return
	}
	defer guardNextcloudResponseWrites(c, leases, func(checkCtx context.Context) error {
		if err := guard.Check(checkCtx, snapshot); err != nil {
			return knowledgeOutputHTTPError(err)
		}
		_, err := h.loadPublishedKnowledge(checkCtx, id)
		return err
	})()
	c.JSON(
		http.StatusOK,
		gin.H{"success": true, "data": page, "total": total, "page": pagination.Page, "page_size": pagination.PageSize},
	)
}
