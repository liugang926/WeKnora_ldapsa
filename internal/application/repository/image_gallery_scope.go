package repository

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// imageGalleryScope restricts the projection before duplicate selection,
// filtering and counting. Only distinct original dependency sets leave SQL.
func (r *chunkRepository) imageGalleryScope(ctx context.Context, tenant uint64, kb string, q *types.ImageAssetQuery) (string, []any, error) {
	db := r.db.WithContext(ctx)
	contributors := db.Table("chunk_images").Select("knowledge_id").Where("tenant_id = ? AND knowledge_base_id = ?", tenant, kb)
	if len(q.KnowledgeIDs) > 0 {
		contributors = contributors.Where("knowledge_id IN ?", q.KnowledgeIDs)
	}
	var origins []string
	err := db.Table("knowledge_input_admissions a").
		Joins("JOIN knowledge_input_material_receipts m ON m.row_kind = 'knowledge' AND m.row_id = a.knowledge_id AND m.admission_id = a.admission_id").
		Where("a.tenant_id = ? AND a.knowledge_base_id = ? AND a.knowledge_id IN (?)", tenant, kb, contributors).
		Distinct("a.source_lineage").Pluck("a.source_lineage", &origins).Error
	if err != nil {
		return "", nil, err
	}
	workspace := access.UseWeKnoraReadAuthority()
	checked := map[types.SourceIdentity]bool{}
	allowed := []string{}
	for _, raw := range origins {
		var lineage types.SourceLineage
		if json.Unmarshal([]byte(raw), &lineage) != nil || lineage.RequireComplete() != nil {
			continue
		}
		visible := true
		for _, source := range lineage.Sources {
			ok, cached := checked[source]
			if !cached {
				ok, err = r.gallerySourceVisible(ctx, source, workspace, q)
				if err != nil {
					return "", nil, err
				}
				checked[source] = ok
			}
			if !ok {
				visible = false
				break
			}
		}
		if visible {
			allowed = append(allowed, raw)
		}
	}
	// Bind the set as one parameter even on galleries with many origins.
	encoded, err := json.Marshal(allowed)
	if err != nil {
		return "", nil, err
	}
	originSet := "SELECT value FROM json_each(?)"
	if r.db.Name() == "postgres" {
		originSet = "SELECT jsonb_array_elements_text(CAST(? AS jsonb))"
	}

	const joins = ` FROM chunk_images i
	JOIN chunks c ON c.id = i.chunk_id AND c.knowledge_id = i.knowledge_id
	 AND c.tenant_id = i.tenant_id AND c.knowledge_base_id = i.knowledge_base_id AND c.deleted_at IS NULL
	JOIN knowledges k ON k.id = i.knowledge_id AND k.knowledge_base_id = i.knowledge_base_id
	 AND k.tenant_id = i.tenant_id AND k.deleted_at IS NULL
	LEFT JOIN knowledge_input_material_receipts kr ON kr.row_kind = 'knowledge' AND kr.row_id = k.id
	LEFT JOIN knowledge_input_admissions a ON a.admission_id = kr.admission_id AND a.knowledge_id = k.id
	 AND a.tenant_id = k.tenant_id AND a.knowledge_base_id = k.knowledge_base_id `
	visible := "a.source_lineage IN (" + originSet + ")"
	current := ""
	parentReceipt := "pr.admission_id = a.admission_id AND pr.row_revision = pv.revision"
	if workspace {
		// Old workspace-owned rows can predate original receipts. A recorded
		// dependency still requires access to its own KB.
		current = " LEFT JOIN knowledge_input_material_receipts cr ON cr.row_kind = 'chunk' AND cr.row_id = c.id "
		parentReceipt = "(pr.row_id IS NULL OR pr.admission_id = a.admission_id)"
		visible = `((kr.row_id IS NULL AND cr.row_id IS NULL AND NOT EXISTS (
			SELECT 1 FROM knowledge_input_admissions prior WHERE prior.knowledge_id = k.id))
			OR ((cr.row_id IS NULL OR cr.admission_id = a.admission_id) AND ` + visible + "))"
	} else {
		current = ` JOIN knowledge_input_row_versions kv ON kv.row_kind = 'knowledge' AND kv.row_id = k.id AND kv.revision = kr.row_revision
		JOIN knowledge_input_material_receipts cr ON cr.row_kind = 'chunk' AND cr.row_id = c.id AND cr.admission_id = a.admission_id
		JOIN knowledge_input_row_versions cv ON cv.row_kind = 'chunk' AND cv.row_id = c.id AND cv.revision = cr.row_revision `
	}
	cte := "WITH visible_images AS NOT MATERIALIZED (SELECT i.*, k.title AS source_name" + joins + current +
		" WHERE i.tenant_id = ? AND i.knowledge_base_id = ? AND " + visible +
		` AND (COALESCE(c.parent_chunk_id, '') = '' OR EXISTS
		 (SELECT 1 FROM chunks p
		  LEFT JOIN knowledge_input_material_receipts pr ON pr.row_kind = 'chunk' AND pr.row_id = p.id
		  LEFT JOIN knowledge_input_row_versions pv ON pv.row_kind = 'chunk' AND pv.row_id = p.id
		  WHERE p.id = c.parent_chunk_id AND p.knowledge_id = c.knowledge_id
		  AND p.tenant_id = c.tenant_id AND p.knowledge_base_id = c.knowledge_base_id AND p.deleted_at IS NULL AND ` + parentReceipt + "))) "
	return cte, []any{tenant, kb, string(encoded)}, nil
}

func (r *chunkRepository) gallerySourceVisible(ctx context.Context, source types.SourceIdentity, workspace bool, q *types.ImageAssetQuery) (bool, error) {
	var row *types.Knowledge
	if workspace {
		row = &types.Knowledge{}
		err := r.db.WithContext(ctx).Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND channel = ?",
			source.KnowledgeID, source.TenantID, source.KnowledgeBaseID, source.Provider).Take(row).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	} else {
		reader := &knowledgeRepository{db: r.db}
		current, identity, err := reader.ResolveNextcloudSourceLineage(ctx, source.TenantID, source.KnowledgeBaseID, source.KnowledgeID)
		if errors.Is(err, ErrNextcloudLineageSnapshotDenied) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if identity != source {
			return false, nil
		}
		row = current
	}
	return q.AuthorizeSource(ctx, row)
}
