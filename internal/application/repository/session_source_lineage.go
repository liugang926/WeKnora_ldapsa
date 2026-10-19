package repository

import (
	"context"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
)

// SaveGeneratedSessionTitle commits text and original input proof in one
// owner-scoped statement. CAS prevents an async title replacing a manual edit.
func (r *sessionRepository) SaveGeneratedSessionTitle(
	ctx context.Context,
	tenantID uint64,
	ownerID, id string,
	expected types.SessionTitleSnapshot,
	title string,
	lineage *types.SourceLineage,
) (int64, error) {
	if lineage.RequireComplete() != nil {
		return 0, types.ErrSourceLineageUnknown
	}
	query := applySessionUserScope(
		r.db.WithContext(ctx).
			Model(&types.Session{}).
			Where(
				"tenant_id = ? AND id = ? AND title = ? AND title_revision = ?",
				tenantID,
				id,
				expected.Text,
				expected.Revision,
			),
		ownerID,
	)
	var originalProof any
	if expected.SourceLineage != nil {
		value, err := expected.SourceLineage.Value()
		if err != nil {
			return 0, err
		}
		originalProof = value
	}
	if r.db.Name() == "postgres" {
		query = query.Where("title_source_lineage IS NOT DISTINCT FROM ?", originalProof)
	} else {
		query = query.Where("title_source_lineage IS ?", originalProof)
	}
	result := query.
		Updates(map[string]any{"title": title, "title_source_lineage": lineage, "updated_at": time.Now()})
	return result.RowsAffected, result.Error
}

// Title matching is only a candidate filter; current authorization happens
// before a title match can affect client items/pagination/total.
func sessionTitleCandidatePredicate(dialect string) string {
	if dialect == "postgres" {
		return ("s.title_source_lineage IS NOT NULL AND s.title_source_lineage->'version' =" +
			" '1'::jsonb AND s.title_source_lineage->>'state' = 'complete' AND jsonb_ty" +
			"peof(s.title_source_lineage->'sources') = 'array'")
	}
	return ("CASE WHEN s.title_source_lineage IS NOT NULL AND json_valid(s.title_source" +
		"_lineage) THEN json_type(s.title_source_lineage, '$.version') = 'integer' " +
		"AND json_extract(s.title_source_lineage, '$.version') = 1 AND json_extract" +
		"(s.title_source_lineage, '$.state') = 'complete' AND json_type(s.title_sou" +
		"rce_lineage, '$.sources') = 'array' ELSE 0 END")
}

func (r *sessionRepository) ListSessionTitleCandidates(
	ctx context.Context,
	query *types.SessionListQuery,
) ([]*types.SessionListItem, error) {
	items, _, err := r.querySessionRows(ctx, query, true)
	return items, err
}
