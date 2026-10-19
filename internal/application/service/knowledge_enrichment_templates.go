package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/readlease"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// New templates live in their KB's proved original configuration. Model
// outputs inherit every template source through the actual artifact admission.
type KnowledgeEnrichmentTemplateService struct {
	store     *repository.KnowledgeInputProvenanceStore
	knowledge interfaces.KnowledgeRepository
	models    interfaces.ModelService
	guard     *access.NextcloudSourceLineageGuard
	leases    readlease.NextcloudReadLeaseStore
}

func NewKnowledgeEnrichmentTemplateService(store *repository.KnowledgeInputProvenanceStore, knowledge interfaces.KnowledgeRepository, models interfaces.ModelService, guard *access.NextcloudSourceLineageGuard, leases readlease.NextcloudReadLeaseStore) *KnowledgeEnrichmentTemplateService {
	return &KnowledgeEnrichmentTemplateService{store, knowledge, models, guard, leases}
}
func (s *KnowledgeEnrichmentTemplateService) checkConfiguration(ctx context.Context, tenant uint64, kb string) error {
	_, lineage, err := s.store.ReadKnowledgeTemplateConfiguration(ctx, tenant, kb)
	if err != nil {
		return err
	}
	if s.guard == nil {
		return types.ErrSourceLineageUnknown
	}
	return s.guard.Check(ctx, lineage, access.LineageReadScope{Targets: types.SearchTargets{&types.SearchTarget{Type: types.SearchTargetTypeKnowledgeBase, TenantID: tenant, KnowledgeBaseID: kb}}})
}
func (s *KnowledgeEnrichmentTemplateService) SetIndependentTemplate(ctx context.Context, tenant uint64, kb, purpose, content string) (*types.KnowledgeBase, error) {
	if err := s.checkConfiguration(ctx, tenant, kb); err != nil {
		return nil, err
	}
	return s.store.WriteIndependentKnowledgeEnrichmentTemplate(ctx, tenant, kb, purpose, content)
}
func (s *KnowledgeEnrichmentTemplateService) InstallCodeTemplate(ctx context.Context, tenant uint64, kb, purpose, literal string) (*types.KnowledgeBase, error) {
	return s.store.WriteCodeKnowledgeEnrichmentTemplate(ctx, tenant, kb, purpose, literal)
}

// GenerateDerivedTemplate reads the selected parents itself. Clients choose
// IDs, never supply cached parent content, digests, actor grants or lineage.
func (s *KnowledgeEnrichmentTemplateService) GenerateDerivedTemplate(ctx context.Context, tenant uint64, kbID, purpose string, parentIDs []string) (*types.KnowledgeBase, error) {
	if err := s.checkConfiguration(ctx, tenant, kbID); err != nil {
		return nil, err
	}

	_, configLineage, err := s.store.ReadKnowledgeTemplateConfiguration(ctx, tenant, kbID)
	if err != nil {
		return nil, err
	}
	docs := []*types.Knowledge{}
	for _, id := range parentIDs {
		k, err := s.knowledge.GetKnowledgeByIDOnly(ctx, id)
		if err != nil || k == nil {
			return nil, types.ErrSourceLineageUnknown
		}
		docs = append(docs, k)
	}
	lease, err := s.beginTemplateRead(ctx, tenant, kbID, configLineage, docs)
	if err != nil {
		return nil, err
	}
	defer lease.Close()
	ctx = lease.Context()
	parents := []*types.ChunkOutputInputSnapshot{}
	seen := map[string]bool{}
	for i, id := range parentIDs {
		if id == "" || seen[id] {
			return nil, types.ErrSourceLineageUnknown
		}
		seen[id] = true

		k := docs[i]
		if err := lease.PinKnowledge(k); err != nil {
			return nil, err
		}
		actual, err := s.store.CaptureChunkOutputInputs(ctx, types.ChunkOutputReadRequest{TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID, AllChunks: true})
		if err != nil {
			return nil, err
		}
		if err := s.guard.Check(ctx, actual.Proof.SourceLineage, access.LineageReadScope{Targets: types.SearchTargets{&types.SearchTarget{Type: types.SearchTargetTypeKnowledgeBase, TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID}}}); err != nil {
			return nil, err
		}
		parents = append(parents, actual)
	}
	input, err := s.store.CaptureKnowledgeEnrichmentTemplateInputs(ctx, tenant, kbID, purpose, parents)
	if err != nil {
		return nil, err
	}
	input, err = s.store.ReadKnowledgeEnrichmentTemplateInputs(ctx, input.ID)
	if err != nil {
		return nil, err
	}

	expanded, err := s.beginTemplateRead(ctx, tenant, kbID, input.Lineage, docs)
	if err != nil {
		return nil, err
	}
	defer expanded.Close()
	ctx = expanded.Context()
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, input.Tenant)
	model, err := s.models.GetChatModel(ctx, input.ModelID)
	if err != nil {
		return nil, err
	}
	parts := []string{}
	for _, parent := range input.Parents {
		parts = append(parts, parent.Knowledge.Title, parent.Knowledge.Description, parent.Knowledge.CustomMetadataText())
		for _, c := range parent.Chunks {
			parts = append(parts, c.Content, c.ImageInfo)
		}
	}
	prompt := fmt.Sprintf("Create a concise %s generation instruction template using only these actual source inputs. Return only the instruction template.\n%s", purpose, strings.Join(parts, "\n"))
	check := func() error {
		if err := lease.Verify(); err != nil {
			return err
		}
		if err := s.store.RecheckKnowledgeEnrichmentTemplateInputs(ctx, input.ID); err != nil {
			return err
		}

		if err := s.guard.Check(ctx, input.Lineage, access.LineageReadScope{Targets: types.SearchTargets{&types.SearchTarget{Type: types.SearchTargetTypeKnowledgeBase, TenantID: tenant, KnowledgeBaseID: kbID}}}); err != nil {
			return err
		}
		if err := expanded.Verify(); err != nil {
			return err
		}
		return s.store.RecheckKnowledgeEnrichmentTemplateInputs(ctx, input.ID)
	}
	if err := check(); err != nil {
		return nil, err
	}
	response, err := model.Chat(types.WithLLMCallMetadata(ctx, "enrichment_template", ""), []chat.Message{{Role: "user", Content: prompt}}, &chat.ChatOptions{Temperature: 0.2, MaxTokens: 2048})
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	if err := check(); err != nil {
		return nil, err
	}
	result, err := s.store.CaptureKnowledgeTemplateModelResult(ctx, input.ID, strings.TrimSpace(response.Content))
	if err != nil {
		return nil, err
	}
	if err := check(); err != nil {
		return nil, err
	}
	return s.store.CommitDerivedKnowledgeEnrichmentTemplate(ctx, result)
}

func requireKnowledgeEnrichmentPromptOrigin(cfg *config.Config, kb *types.KnowledgeBase, purpose string) error {
	if kb != nil && kb.EnrichmentTemplates.Content(purpose) != "" {
		return nil
	}
	_, err := config.KnowledgeEnrichmentPromptLineage(cfg, purpose)
	return err
}
func actualKnowledgeEnrichmentPrompt(cfg *config.Config, kb *types.KnowledgeBase, purpose string) string {
	if kb != nil {
		if content := kb.EnrichmentTemplates.Content(purpose); content != "" {
			return content
		}
	}
	if cfg == nil || cfg.Conversation == nil {
		return ""
	}
	if purpose == "summary" {
		return cfg.Conversation.GenerateSummaryPrompt
	}
	return cfg.Conversation.GenerateQuestionsPrompt
}

type KnowledgeEnrichmentTemplateReadSnapshot struct {
	Content string
	Check   func(context.Context) error
	Close   func() error
}

func (s *KnowledgeEnrichmentTemplateService) beginTemplateRead(ctx context.Context, tenant uint64, kbID string, lineage *types.SourceLineage, docs []*types.Knowledge) (*readlease.NextcloudReadGuard, error) {
	scopes := []readlease.NextcloudKBReadScope{{TenantID: tenant, KBID: kbID}}
	for _, k := range docs {
		scopes = append(scopes, readlease.NextcloudKBReadScope{TenantID: k.TenantID, KBID: k.KnowledgeBaseID})
	}
	if lineage.RequireComplete() != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	for _, source := range lineage.Sources {
		scopes = append(scopes, readlease.NextcloudKBReadScope{TenantID: source.TenantID, KBID: source.KnowledgeBaseID})
	}
	lease, err := readlease.BeginNextcloudKBRead(ctx, s.leases, scopes)
	if err != nil {
		return nil, err
	}
	for _, source := range lineage.Sources {
		k, e := s.knowledge.GetKnowledgeByIDOnly(lease.Context(), source.KnowledgeID)
		if e != nil || k == nil || k.TenantID != source.TenantID || k.KnowledgeBaseID != source.KnowledgeBaseID {
			_ = lease.Close()
			return nil, types.ErrSourceLineageUnknown
		}
		if e = lease.PinKnowledge(k); e != nil {
			_ = lease.Close()
			return nil, e
		}
	}
	return lease, nil
}
func (s *KnowledgeEnrichmentTemplateService) ReadTemplate(ctx context.Context, tenant uint64, id, purpose string) (*KnowledgeEnrichmentTemplateReadSnapshot, error) {
	kb, lineage, err := s.store.ReadKnowledgeTemplateConfiguration(ctx, tenant, id)
	if err != nil {
		return nil, err
	}
	if err := s.checkConfiguration(ctx, tenant, id); err != nil {
		return nil, err
	}

	lease, err := s.beginTemplateRead(ctx, tenant, id, lineage, nil)
	if err != nil {
		return nil, err
	}
	completed := false
	defer func() {
		if !completed {
			_ = lease.Close()
		}
	}()
	ctx = lease.Context()
	content := kb.EnrichmentTemplates.Content(purpose)
	revision, controlDigest, err := s.store.KnowledgeTemplateConfigurationStamp(ctx, tenant, id)
	if err != nil {
		return nil, err
	}
	local := func(checkCtx context.Context) error {
		currentRevision, currentDigest, err := s.store.KnowledgeTemplateConfigurationStamp(checkCtx, tenant, id)
		if err != nil {
			return err
		}
		if currentRevision != revision || currentDigest != controlDigest {
			return repository.ErrKnowledgeInputChanged
		}
		current, currentLineage, err := s.store.ReadKnowledgeTemplateConfiguration(checkCtx, tenant, id)
		if err != nil {
			return err
		}
		a, err := repository.KnowledgeInputDigest(kb.EnrichmentTemplates)
		if err != nil {
			return err
		}
		b, err := repository.KnowledgeInputDigest(current.EnrichmentTemplates)
		if err != nil {
			return err
		}
		if a != b || current.EnrichmentTemplates.Content(purpose) != content {
			return repository.ErrKnowledgeInputChanged
		}
		a, err = repository.KnowledgeInputDigest(lineage)
		if err != nil {
			return err
		}
		b, err = repository.KnowledgeInputDigest(currentLineage)
		if err != nil {
			return err
		}
		if a != b {
			return repository.ErrKnowledgeInputChanged
		}

		return nil
	}
	check := func(checkCtx context.Context) error {
		if err := lease.Verify(); err != nil {
			return err
		}
		if err := local(checkCtx); err != nil {
			return err
		}
		if err := s.checkConfiguration(checkCtx, tenant, id); err != nil {
			return err
		}
		if err := lease.Verify(); err != nil {
			return err
		}
		return local(checkCtx)
	}
	completed = true
	return &KnowledgeEnrichmentTemplateReadSnapshot{Content: content, Check: check, Close: lease.Close}, nil
}
