package repository

import (
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// NextcloudNoRetrievableContentCode is safe to return to the source's Files
// sidebar. It contains no parser output, source text, paths, or credentials.
const NextcloudNoRetrievableContentCode = "no_retrievable_content"

// NextcloudTextChunkRequired limits this publication check to formats whose
// content must produce a text chunk. Images, PDF/office documents, and CSV
// can instead depend on multimodal or format-specific summary processing.
func NextcloudTextChunkRequired(fileType string) bool {
	switch strings.ToLower(strings.TrimPrefix(strings.TrimSpace(fileType), ".")) {
	case "txt", "md", "markdown", "html", "htm", "mhtml", "json":
		return true
	default:
		return false
	}
}

// nextcloudHasRetrievableTextChunk is a necessary local-content check, not
// proof that every configured external vector or keyword index is healthy.
// The processing pipeline must still finish its configured BatchIndex before
// a candidate can become completed and eligible for publication.
func nextcloudHasRetrievableTextChunk(tx *gorm.DB, candidate *types.Knowledge) (bool, error) {
	var chunk struct{ ID string }
	result := tx.Table("chunks").Select("id").Where(
		("tenant_id = ? AND knowledge_base_id = ? AND knowledge_i" +
			"d = ? AND deleted_at IS NULL AND chunk_type = ? AND is_" +
			"enabled = ? AND index_status = ? AND TRIM(content) <> '" +
			"'"),
		candidate.TenantID, candidate.KnowledgeBaseID, candidate.ID,
		types.ChunkTypeText, true, "ready",
	).Limit(1).Find(&chunk)
	return result.RowsAffected == 1, result.Error
}
