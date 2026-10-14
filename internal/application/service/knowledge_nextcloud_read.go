package service

import (
	"context"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/application/readlease"
	"github.com/Tencent/WeKnora/internal/types"
)

// agentReadScopes resolves the server-owned search targets again. A stale
// model-visible handle, revoked KB share or directory grant cannot turn a
// durable lease into permission to read content.
func (s *knowledgeService) agentReadScopes(ctx context.Context,
	targets types.SearchTargets,
) ([]readlease.NextcloudKBReadScope, error) {
	if s.kbService == nil {
		return nil, fmt.Errorf("knowledge base service unavailable")
	}
	permissions := kbReadPermissions(ctx, s.kbShareService)
	seen := make(map[string]bool, len(targets))
	scopes := make([]readlease.NextcloudKBReadScope, 0, len(targets))
	for _, target := range targets {
		if target == nil || target.KnowledgeBaseID == "" || seen[target.KnowledgeBaseID] {
			continue
		}
		seen[target.KnowledgeBaseID] = true
		kb, err := s.kbService.GetKnowledgeBaseByIDOnly(ctx, target.KnowledgeBaseID)
		if err != nil || kb == nil || kb.ID != target.KnowledgeBaseID ||
			kb.TenantID == 0 || kb.TenantID != target.TenantID {
			return nil, fmt.Errorf("knowledge base %s is unavailable", target.KnowledgeBaseID)
		}
		allowed, err := permissions.Check(kb.ID, kb.TenantID, types.OrgRoleViewer)
		if err != nil || !allowed {
			return nil, fmt.Errorf("knowledge base %s read grant is unavailable", kb.ID)
		}
		if s.groupAccess != nil {
			grant, err := s.groupAccess.EffectivePermission(ctx, kb.TenantID,
				types.GroupResourceTypeKnowledgeBase, kb.ID,
				types.ResourceActionRead, time.Now().UTC())
			if err != nil || !grant.Allowed {
				return nil, fmt.Errorf("knowledge base %s group read grant is unavailable", kb.ID)
			}
		}
		scopes = append(scopes, readlease.NextcloudKBReadScope{TenantID: kb.TenantID, KBID: kb.ID})
	}
	return scopes, nil
}

// BeginNextcloudRead protects Agent document/metadata/tool result reads from
// physical GC. The caller must Close the returned guard and recheck both its
// lease and live grants before making the tool result visible.
func (s *knowledgeService) BeginNextcloudRead(ctx context.Context,
	targets types.SearchTargets,
) (*readlease.NextcloudReadGuard, error) {
	scopes, err := s.agentReadScopes(ctx, targets)
	if err != nil {
		return nil, err
	}
	return readlease.BeginNextcloudKBRead(ctx, s.contentLeases, scopes)
}

func (s *knowledgeService) CheckNextcloudReadTargets(ctx context.Context,
	targets types.SearchTargets,
) error {
	_, err := s.agentReadScopes(ctx, targets)
	return err
}
