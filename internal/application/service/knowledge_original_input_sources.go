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
	paths      access.NextcloudSourcePathResolver
	checkBuild func(context.Context, *types.DataSourceConfig, string, string, int64, string, string) error
}

// ConfigureDataSourceOriginalInputSources configures the data source original input sources boundary.
func ConfigureDataSourceOriginalInputSources(
	source interfaces.DataSourceService,
	store *repository.KnowledgeInputProvenanceStore,
) {
	if s, ok := source.(*DataSourceService); ok {
		s.sourceInputs = store
	}
}

// NewKnowledgeOriginalInputSourceVerifier constructs the configured KnowledgeOriginalInputSourceVerifier.
func NewKnowledgeOriginalInputSourceVerifier(
	store *repository.KnowledgeInputProvenanceStore,
	guard *access.NextcloudSourceLineageGuard,
	sources access.NextcloudDataSourceLookup,
	paths ...access.NextcloudSourcePathResolver,
) repository.KnowledgeOriginalInputSourceVerifier {
	verifier := &knowledgeOriginalInputSourceVerifier{
		store:      store,
		public:     guard,
		sources:    sources,
		checkBuild: nextcloud.CheckCurrentPublication,
	}
	if len(paths) == 1 {
		verifier.paths = paths[0]
	}
	return verifier
}

func (v *knowledgeOriginalInputSourceVerifier) BeginKnowledgeConfigurationSources(
	ctx context.Context,
	tenant uint64,
	kb string,
	lineage *types.SourceLineage,
) (context.Context, func(), error) {
	noop := func() {}
	if v == nil || v.store == nil || lineage.RequireComplete() != nil {
		return ctx, noop, types.ErrSourceLineageUnknown
	}
	var read repository.ScopedOnboardingConfigurationRead
	var err error
	if len(lineage.Sources) == 0 {
		read, err = v.store.BeginScopedIndependentConfigurationRead(ctx, tenant, kb, lineage)
	} else {
		store := v.store.NextcloudOnboardingConfigurationStore()
		if store == nil {
			return ctx, noop, access.ErrNextcloudPublicationUnavailable
		}
		read, err = store.BeginScopedOnboardingConfigurationRead(ctx, tenant, kb, lineage)
	}
	if err != nil {
		return ctx, noop, err
	}
	if read == nil {
		return ctx, noop, access.ErrNextcloudPublicationUnavailable
	}
	if err := repository.HoldScopedOnboardingConfigurationRead(ctx, tenant, kb, lineage, read); err != nil {
		_ = read.Close()
		return ctx, noop, err
	}
	current := read.Context(ctx)
	if current == nil {
		repository.CloseScopedOnboardingConfigurationReads(ctx)
		return ctx, noop, access.ErrNextcloudPublicationUnavailable
	}
	return current, func() { repository.CloseScopedOnboardingConfigurationReads(ctx) }, nil
}

func (v *knowledgeOriginalInputSourceVerifier) CheckKnowledgeConfigurationSources(
	ctx context.Context,
	tenant uint64,
	kb string,
	lineage *types.SourceLineage,
) error {
	if handled, err := repository.CheckScopedOnboardingConfigurationSources(
		ctx,
		tenant,
		kb,
		lineage,
		false,
	); handled ||
		err != nil {
		return err
	}
	if v == nil || v.public == nil {
		return types.ErrSourceLineageUnknown
	}
	return v.public.Check(
		ctx,
		lineage,
		access.LineageReadScope{
			Targets: types.SearchTargets{
				&types.SearchTarget{Type: types.SearchTargetTypeKnowledgeBase, TenantID: tenant, KnowledgeBaseID: kb},
			},
		},
	)
}

func (v *knowledgeOriginalInputSourceVerifier) CheckKnowledgeOriginalInputSources(
	ctx context.Context,
	proof *types.KnowledgeBuildInputProof,
) error {
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
			if saved.TenantID != proof.TenantID || saved.KnowledgeBaseID != proof.KnowledgeBaseID ||
				saved.KnowledgeID != proof.KnowledgeID {
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
			if ds == nil || ds.ID != saved.DataSourceID || ds.TenantID != saved.TenantID ||
				ds.KnowledgeBaseID != saved.KnowledgeBaseID ||
				ds.Type != types.ConnectorTypeNextcloud ||
				ds.Status != types.DataSourceStatusActive {
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
			path := k.GetMetadata()["nextcloud_path"]
			if v.paths != nil {
				var err error
				path, err = v.paths.ResolveCurrentSourcePath(
					ctx,
					saved.TenantID,
					saved.KnowledgeBaseID,
					saved.DataSourceID,
					path,
				)
				if err != nil {
					return err
				}
			}
			if err := v.checkBuild(
				types.WithNextcloudContentRevision(ctx, k.GetMetadata()["nextcloud_content_revision"]),
				cfg,
				saved.InstanceID,
				saved.BindingID,
				id,
				saved.ETag,
				path,
			); err != nil {
				return err
			}
		}
		if !found {
			return access.ErrNextcloudPublicationDenied
		}
		lineage.Sources = remaining
		if handled, err := repository.CheckScopedOnboardingConfigurationSources(
			ctx,
			proof.TenantID,
			proof.KnowledgeBaseID,
			lineage,
			true,
		); handled ||
			err != nil {
			return err
		}
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
	return v.public.Check(
		ctx,
		lineage,
		access.LineageReadScope{
			Targets: types.SearchTargets{
				&types.SearchTarget{
					Type:            types.SearchTargetTypeKnowledgeBase,
					TenantID:        proof.TenantID,
					KnowledgeBaseID: proof.KnowledgeBaseID,
				},
			},
		},
	)
}
