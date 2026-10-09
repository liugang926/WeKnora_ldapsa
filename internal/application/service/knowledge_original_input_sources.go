package service

import (
	"context"
	"strconv"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type knowledgeOriginalInputSourceVerifier struct {
	store      *repository.KnowledgeInputProvenanceStore
	public     *access.NextcloudSourceLineageGuard
	sources    access.NextcloudDataSourceLookup
	checkBuild func(context.Context, *types.DataSourceConfig, string, string, int64, string, string) error
}

func ConfigureDataSourceOriginalInputSources(source interfaces.DataSourceService, store *repository.KnowledgeInputProvenanceStore) {
	if s, ok := source.(*DataSourceService); ok {
		s.sourceInputs = store
	}
}

func NewKnowledgeOriginalInputSourceVerifier(store *repository.KnowledgeInputProvenanceStore, guard *access.NextcloudSourceLineageGuard, sources access.NextcloudDataSourceLookup) repository.KnowledgeOriginalInputSourceVerifier {
	return &knowledgeOriginalInputSourceVerifier{store: store, public: guard, sources: sources, checkBuild: nextcloud.CheckCurrentPublication}
}
func (v *knowledgeOriginalInputSourceVerifier) CheckKnowledgeConfigurationSources(ctx context.Context, tenant uint64, kb string, lineage *types.SourceLineage) error {
	if v == nil || v.public == nil {
		return types.ErrSourceLineageUnknown
	}
	return v.public.Check(ctx, lineage, access.LineageReadScope{Targets: types.SearchTargets{&types.SearchTarget{Type: types.SearchTargetTypeKnowledgeBase, TenantID: tenant, KnowledgeBaseID: kb}}})
}
func (v *knowledgeOriginalInputSourceVerifier) CheckKnowledgeOriginalInputSources(ctx context.Context, proof *types.KnowledgeBuildInputProof) error {
	if v == nil || v.store == nil || proof == nil || proof.SourceLineage.RequireComplete() != nil {
		return types.ErrSourceLineageUnknown
	}
	lineage, err := types.UnionSourceLineage(proof.SourceLineage)
	if err != nil {
		return err
	}
	if repository.NextcloudBuildLeasePresent(ctx, proof.TenantID, proof.KnowledgeBaseID, proof.KnowledgeID) {
		k, err := v.store.ReadOriginalNextcloudBuildSource(ctx, proof)
		if err != nil {
			return err
		}
		if v.sources == nil || v.checkBuild == nil {
			return access.ErrNextcloudPublicationUnavailable
		}
		remaining := []types.SourceIdentity{}
		found := false
		for _, saved := range lineage.Sources {
			if saved.TenantID != proof.TenantID || saved.KnowledgeBaseID != proof.KnowledgeBaseID || saved.KnowledgeID != proof.KnowledgeID {
				remaining = append(remaining, saved)
				continue
			}
			if found {
				return repository.ErrKnowledgeInputChanged
			}
			found = true
			ds, err := v.sources.FindByID(ctx, saved.DataSourceID)
			if err != nil {
				return err
			}
			if ds == nil || ds.ID != saved.DataSourceID || ds.TenantID != saved.TenantID || ds.KnowledgeBaseID != saved.KnowledgeBaseID || ds.Type != types.ConnectorTypeNextcloud || ds.Status != types.DataSourceStatusActive {
				return access.ErrNextcloudPublicationDenied
			}
			cfg, err := ds.ParseConfig()
			if err != nil {
				return err
			}
			if cfg == nil || len(cfg.ResourceIDs) != 1 || cfg.ResourceIDs[0] != saved.BindingID {
				return access.ErrNextcloudPublicationDenied
			}
			cfg.Type = ds.Type
			id, err := strconv.ParseInt(saved.FileID, 10, 64)
			if err != nil || id <= 0 {
				return access.ErrNextcloudPublicationDenied
			}
			if err := v.checkBuild(ctx, cfg, saved.InstanceID, saved.BindingID, id, saved.ETag, k.GetMetadata()["nextcloud_path"]); err != nil {
				return err
			}
		}
		if !found {
			return access.ErrNextcloudPublicationDenied
		}
		lineage.Sources = remaining
		// This exact staging source uses a machine build lease and signed source
		// policy check. Inherited public sources still need the real actor.
		if len(remaining) == 0 {
			_, err = v.store.ReadOriginalNextcloudBuildSource(ctx, proof)
			return err
		}
	}
	if v.public == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	return v.public.Check(ctx, lineage, access.LineageReadScope{Targets: types.SearchTargets{&types.SearchTarget{Type: types.SearchTargetTypeKnowledgeBase, TenantID: proof.TenantID, KnowledgeBaseID: proof.KnowledgeBaseID}}})
}
