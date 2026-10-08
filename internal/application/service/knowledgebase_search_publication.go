package service

import (
	"context"
	"reflect"

	"github.com/Tencent/WeKnora/internal/types"
)

func (s *knowledgeBaseService) checkSearchPublication(ctx context.Context, k *types.Knowledge) error {
	if s.searchPublicationCheck != nil {
		return s.searchPublicationCheck(ctx, k)
	}
	return s.publicationGuard.CheckKnowledge(ctx, k)
}

func nextcloudSearchResult(r *types.SearchResult) bool {
	if r == nil {
		return false
	}
	return r.KnowledgeChannel == types.ConnectorTypeNextcloud ||
		r.Metadata["datasource_id"] != "" || r.Metadata["nextcloud_instance_id"] != "" ||
		r.Metadata["nextcloud_binding_id"] != "" || r.Metadata["nextcloud_file_id"] != ""
}

// filterSearchResultsAtPublicationBoundary runs after all chunk and image
// hydration. A result assembled under an older source version or grant is
// omitted even if it passed the earlier pre-hydration authorization.
func (s *knowledgeBaseService) filterSearchResultsAtPublicationBoundary(
	ctx context.Context, results []*types.SearchResult,
) ([]*types.SearchResult, error) {
	if len(results) == 0 {
		return results, nil
	}
	allowed := make([]*types.SearchResult, 0, len(results))
	checked := make(map[string]bool)
	currentRows := make(map[string]*types.Knowledge)
	permitted := make(map[string]bool)
	permissions := kbReadPermissions(ctx, s.kbShareService)
	for _, result := range results {
		if result == nil {
			continue
		}
		if !nextcloudSearchResult(result) {
			allowed = append(allowed, result)
			continue
		}
		id := result.KnowledgeID
		if !checked[id] {
			checked[id] = true
			if s.kgRepo != nil && id != "" {
				current, err := s.kgRepo.GetKnowledgeByIDOnly(ctx, id)
				if err == nil && current != nil && current.ID == id {
					currentRows[id] = current
					grant, grantErr := permissions.Check(current.KnowledgeBaseID, current.TenantID, types.OrgRoleViewer)
					if grantErr == nil && grant && s.checkSearchPublication(ctx, current) == nil {
						permitted[id] = true
					}
				}
			}
		}
		current := currentRows[id]
		if permitted[id] && current != nil &&
			current.KnowledgeBaseID == result.KnowledgeBaseID &&
			current.Channel == result.KnowledgeChannel &&
			reflect.DeepEqual(current.GetMetadata(), result.Metadata) {
			allowed = append(allowed, result)
		}
	}
	return allowed, nil
}
